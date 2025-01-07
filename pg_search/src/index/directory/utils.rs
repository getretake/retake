use crate::index::mvcc::MvccSatisfies;
use crate::postgres::storage::block::{
    DeleteEntry, FileEntry, LinkedList, MVCCEntry, PgItem, SegmentFileDetails, SegmentMetaEntry,
    SCHEMA_START, SEGMENT_METAS_START, SETTINGS_START,
};
use crate::postgres::storage::{LinkedBytesList, LinkedItemList};
use anyhow::Result;
use pgrx::pg_sys;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tantivy::index::SegmentComponent;
use tantivy::{
    index::{DeleteMeta, IndexSettings, InnerSegmentMeta, SegmentId, SegmentMetaInventory},
    schema::Schema,
    IndexMeta,
};

pub unsafe fn list_managed_files(relation_oid: pg_sys::Oid) -> tantivy::Result<HashSet<PathBuf>> {
    let segment_components =
        LinkedItemList::<SegmentMetaEntry>::open(relation_oid, SEGMENT_METAS_START);
    let bman = segment_components.bman();
    let mut blockno = segment_components.get_start_blockno();
    let mut files = HashSet::new();

    while blockno != pg_sys::InvalidBlockNumber {
        let buffer = bman.get_buffer(blockno);
        let page = buffer.page();
        let max_offset = page.max_offset_number();
        let mut offsetno = pg_sys::FirstOffsetNumber;

        while offsetno <= max_offset {
            if let Some((entry, _)) = page.read_item::<SegmentMetaEntry>(offsetno) {
                files.extend(entry.get_component_paths());
            }
            offsetno += 1;
        }

        blockno = page.next_blockno();
    }

    Ok(files)
}

pub fn save_schema(relation_oid: pg_sys::Oid, tantivy_schema: &Schema) -> Result<()> {
    let mut schema = LinkedBytesList::open(relation_oid, SCHEMA_START);
    if schema.is_empty() {
        let bytes = serde_json::to_vec(tantivy_schema)?;
        unsafe {
            let _ = schema.write(&bytes)?;
        }
    }
    Ok(())
}

pub fn save_settings(relation_oid: pg_sys::Oid, tantivy_settings: &IndexSettings) -> Result<()> {
    let mut settings = LinkedBytesList::open(relation_oid, SETTINGS_START);
    if settings.is_empty() {
        let bytes = serde_json::to_vec(tantivy_settings)?;
        unsafe {
            let _ = settings.write(&bytes)?;
        }
    }
    Ok(())
}

pub unsafe fn save_new_metas(
    relation_oid: pg_sys::Oid,
    new_meta: &IndexMeta,
    prev_meta: &IndexMeta,
    directory_entries: &mut FxHashMap<PathBuf, FileEntry>,
) -> Result<()> {
    let current_xid = pg_sys::GetCurrentTransactionId();
    let mut linked_list =
        LinkedItemList::<SegmentMetaEntry>::open(relation_oid, SEGMENT_METAS_START);

    let incoming_segments = new_meta
        .segments
        .iter()
        .map(|s| (s.id(), s))
        .collect::<FxHashMap<_, _>>();
    let new_ids = new_meta
        .segments
        .iter()
        .map(|s| s.id())
        .collect::<FxHashSet<_>>();
    let previous_ids = prev_meta
        .segments
        .iter()
        .map(|s| s.id())
        .collect::<FxHashSet<_>>();

    // first, reorganize the directory_entries by segment id
    let mut new_files =
        FxHashMap::<SegmentId, FxHashMap<SegmentComponent, (FileEntry, PathBuf)>>::default();

    for (path, file_entry) in directory_entries.drain() {
        let segment_id = path.segment_id();
        let component_type = path.component_type();

        if let (Some(segment_id), Some(component_type)) = (segment_id, component_type) {
            new_files
                .entry(segment_id)
                .or_default()
                .insert(component_type, (file_entry, path));
        } else {
            panic!("malformed PathBuf: {}", path.display());
        }
    }

    let created_ids = new_ids.difference(&previous_ids).collect::<Vec<_>>();
    let mut modified_ids = previous_ids.intersection(&new_ids).collect::<Vec<_>>();
    let deleted_ids = previous_ids.difference(&new_ids).collect::<Vec<_>>();

    modified_ids.retain(|id| new_files.contains_key(id));

    //
    // process the new segments
    //
    // these are added to the linked list as new items
    // xmin is set to the current transaction id
    // and xmax is set to InvalidTransactionId
    //
    let created_entries = created_ids
        .into_iter()
        .filter_map(|id| {
            let created_segment = incoming_segments.get(id).unwrap();
            let mut files = new_files.remove(id)?;

            let meta_entry = SegmentMetaEntry {
                segment_id: *id,
                max_doc: created_segment.max_doc(),
                xmin: current_xid,
                xmax: pg_sys::InvalidTransactionId,
                postings: files.remove(&SegmentComponent::Postings).map(|e| e.0),
                positions: files.remove(&SegmentComponent::Positions).map(|e| e.0),
                fast_fields: files.remove(&SegmentComponent::FastFields).map(|e| e.0),
                field_norms: files.remove(&SegmentComponent::FieldNorms).map(|e| e.0),
                terms: files.remove(&SegmentComponent::Terms).map(|e| e.0),
                store: files.remove(&SegmentComponent::Store).map(|e| e.0),
                temp_store: files.remove(&SegmentComponent::TempStore).map(|e| e.0),
                delete: files
                    .remove(&SegmentComponent::Delete)
                    .map(|(file_entry, _)| DeleteEntry {
                        file_entry,
                        num_deleted_docs: created_segment.num_deleted_docs(),
                    }),
            };

            Some(meta_entry)
        })
        .collect::<Vec<_>>();

    //
    // process the modified segments
    //
    // lookup existing version, update the `delete` entry
    // if it already has one also locate its LinkedBytesList and .mark_deleted()
    // xmin/xmax do not change
    //
    let mut replaced_delete_entries = Vec::new();
    let modified_entries = modified_ids
        .into_iter()
        .filter_map(|id| {
            let mut files = new_files.remove(id)?;
            assert!(
                files.len() == 1 && files.contains_key(&SegmentComponent::Delete),
                "new files for segment_id `{id}` should be exactly one Delete component:  {files:#?}"
            );

            let existing_segment = incoming_segments.get(id).unwrap();
            let (mut meta_entry, blockno, _) = linked_list
                .lookup_ex(|entry| entry.segment_id == *id)
                .unwrap_or_else(|e| {
                    panic!("segment id `{id}` should be in the segment meta linked list:  {e}")
                });
            let (new_delete_entry, _path) = files
                .remove(&SegmentComponent::Delete)
                .unwrap_or_else(|| panic!("missing new delete file for segment_id `{id}`"));

            if meta_entry.delete.is_some() {
                // remember the old delete_entry for future action
                replaced_delete_entries.push(meta_entry.delete.unwrap());
            }

            // replace (or set new) the delete_entry
            meta_entry.delete = Some(DeleteEntry {
                file_entry: new_delete_entry,
                num_deleted_docs: existing_segment.num_deleted_docs(),
            });

            Some((meta_entry, blockno))
        })
        .collect::<Vec<_>>();

    //
    // process the deleted segments
    //
    // find the deleted segment entries and set their `xmax` to this transaction id
    // and for each file in each deleted segment, locate its LinkedBytesList and .mark_deleted()
    //
    let deleted_entries = deleted_ids
        .into_iter()
        .map(|id| {
            let (mut meta_entry, blockno, _) = linked_list
                .lookup_ex(|entry| entry.segment_id == *id)
                .unwrap_or_else(|e| {
                    panic!("segment id `{id}` should be in the segment meta linked list: {e}")
                });

            meta_entry.xmax = current_xid;
            (meta_entry, blockno)
        })
        .collect::<Vec<_>>();

    //
    // recycle anything leftover in `new_files` to our input `directory_entries` as they belong to segment(s) we
    // are not dealing with in this call to save_new_metas()
    //

    if !new_files.is_empty() {
        directory_entries.extend(
            new_files
                .into_values()
                .flat_map(|segment| segment.into_values())
                .map(|(entry, path)| (path, entry)),
        )
    }

    //
    // now change things on disk
    //

    // delete old entries and their corresponding files
    for (entry, blockno) in deleted_entries {
        assert!(entry.xmax == current_xid);

        let mut buffer = linked_list.bman_mut().get_buffer_mut(blockno);
        let mut page = buffer.page_mut();

        let Some(offno) =
            page.find_item::<SegmentMetaEntry, _>(|item| item.segment_id == entry.segment_id)
        else {
            panic!(
                "DELETE:  could not find SegmentMetaEntry for segment_id `{}` on block #{blockno}",
                entry.segment_id
            );
        };

        let PgItem(pg_item, size) = entry.into();

        let did_replace = page.replace_item(offno, pg_item, size);
        assert!(did_replace);
        drop(buffer);

        for file_entry in [
            entry.postings,
            entry.positions,
            entry.fast_fields,
            entry.field_norms,
            entry.terms,
            entry.store,
            entry.temp_store,
            entry.delete.map(|de| de.file_entry),
        ]
        .into_iter()
        .flatten()
        {
            let mut file = LinkedBytesList::open(relation_oid, file_entry.staring_block);
            file.mark_deleted();
        }
    }

    // replace the modified entries
    for (entry, blockno) in modified_entries {
        let mut buffer = linked_list.bman_mut().get_buffer_mut(blockno);
        let mut page = buffer.page_mut();
        let Some(offno) =
            page.find_item::<SegmentMetaEntry, _>(|item| item.segment_id == entry.segment_id)
        else {
            panic!(
                "MODIFY:  could not find SegmentMetaEntry for segment_id `{}` on block #{blockno}",
                entry.segment_id
            );
        };

        let PgItem(pg_item, size) = entry.into();
        let did_replace = page.replace_item(offno, pg_item, size);
        if !did_replace {
            // couldn't replace because it doesn't fit in that slot, so delete the item...
            page.delete_item(offno);

            // ... and add it to somewhere in the list, starting on this page
            linked_list.add_items(vec![entry], Some(buffer))?;
        }
    }
    // chase down the linked lists for any existing deleted entries and mark them as deleted
    for deleted_entry in replaced_delete_entries {
        let mut file = LinkedBytesList::open(relation_oid, deleted_entry.file_entry.staring_block);
        file.mark_deleted();
    }

    // add the new entries
    linked_list.add_items(created_entries, None)?;

    Ok(())
}

pub unsafe fn load_metas(
    relation_oid: pg_sys::Oid,
    inventory: &SegmentMetaInventory,
    snapshot: pg_sys::Snapshot,
    solve_mvcc: MvccSatisfies,
) -> tantivy::Result<IndexMeta> {
    let segment_metas = LinkedItemList::<SegmentMetaEntry>::open(relation_oid, SEGMENT_METAS_START);
    let heap_oid = unsafe { pg_sys::IndexGetRelation(relation_oid, false) };
    let heap_relation = unsafe { pg_sys::RelationIdGetRelation(heap_oid) };
    let mut alive_segments = vec![];
    let mut opstamp = None;
    let mut blockno = segment_metas.get_start_blockno();

    let bman = segment_metas.bman();

    while blockno != pg_sys::InvalidBlockNumber {
        let buffer = bman.get_buffer(blockno);
        let page = buffer.page();
        let max_offset = page.max_offset_number();
        let mut offsetno = pg_sys::FirstOffsetNumber;

        while offsetno <= max_offset {
            if let Some((entry, _)) = page.read_item::<SegmentMetaEntry>(offsetno) {
                if (solve_mvcc == MvccSatisfies::Any && !entry.recyclable(snapshot, heap_relation))
                    || entry.visible(snapshot)
                {
                    let inner_segment_meta = InnerSegmentMeta {
                        max_doc: entry.max_doc,
                        segment_id: entry.segment_id,
                        deletes: entry.delete.map(|delete_entry| DeleteMeta {
                            num_deleted_docs: delete_entry.num_deleted_docs,
                            opstamp: 0, // hardcode zero as the entry's opstamp as it's not used
                        }),
                        include_temp_doc_store: Arc::new(AtomicBool::new(false)),
                    };
                    alive_segments.push(inner_segment_meta.track(inventory));

                    opstamp = opstamp.max(Some(entry.opstamp()));
                }
            }

            offsetno += 1;
        }

        blockno = page.next_blockno();
    }

    pg_sys::RelationClose(heap_relation);

    let schema = LinkedBytesList::open(relation_oid, SCHEMA_START);
    let settings = LinkedBytesList::open(relation_oid, SETTINGS_START);
    let deserialized_schema = serde_json::from_slice(&schema.read_all())?;
    let deserialized_settings = serde_json::from_slice(&settings.read_all())?;

    Ok(IndexMeta {
        segments: alive_segments,
        schema: deserialized_schema,
        index_settings: deserialized_settings,
        opstamp: opstamp.unwrap_or(0),
        payload: None,
    })
}
