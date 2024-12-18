// Copyright (c) 2023-2024 Retake, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use anyhow::Result;
use pgrx::{pg_sys, PgRelation};
use std::sync::Arc;
use tantivy::indexer::{MergePolicy, NoMergePolicy, UserOperation};
use tantivy::schema::Field;
use tantivy::{
    Ctid, Index, IndexSettings, IndexWriter, Opstamp, TantivyDocument, TantivyError, Term,
};
use thiserror::Error;

use crate::index::bulk_delete::BulkDeleteDirectory;
use crate::index::channel::{ChannelDirectory, ChannelRequestHandler, NeedWal};
use crate::index::merge_policy::MergeLock;
use crate::index::mvcc::MVCCDirectory;
use crate::index::{get_index_schema, setup_tokenizers, BlockDirectoryType, WriterResources};
use crate::postgres::options::SearchIndexCreateOptions;
use crate::{
    postgres::types::TantivyValueError,
    schema::{SearchDocument, SearchIndexSchema},
};

// NB:  should this be a GUC?  Could be useful or could just complicate things for the user
/// How big should our insert queue get before we go ahead and add them to the tantivy index?
const MAX_INSERT_QUEUE_SIZE: usize = 1000;
const CHANNEL_QUEUE_LEN: usize = 1000;

/// The entity that interfaces with Tantivy indexes.
pub struct SearchIndexWriter {
    pub schema: SearchIndexSchema,
    ctid_field: Field,

    // keep all these private -- leaking them to the public API would allow callers to
    // mis-use the IndexWriter in particular.
    writer: Arc<IndexWriter>,
    handler: ChannelRequestHandler,
    wants_merge: bool,
    insert_queue: Vec<UserOperation>,
    relation_oid: pg_sys::Oid,
    need_wal: NeedWal,
}

impl SearchIndexWriter {
    pub fn open(
        index_relation: &PgRelation,
        directory_type: BlockDirectoryType,
        resources: WriterResources,
    ) -> Result<Self> {
        let schema = get_index_schema(index_relation)?;
        let create_options = index_relation.rd_options as *mut SearchIndexCreateOptions;
        let (parallelism, memory_budget, wants_merge, merge_policy, need_wal) =
            resources.resources(unsafe { &*create_options });

        let (req_sender, req_receiver) = crossbeam::channel::bounded(CHANNEL_QUEUE_LEN);
        let channel_dir = ChannelDirectory::new(req_sender);
        let mut handler = match directory_type {
            BlockDirectoryType::Mvcc => ChannelRequestHandler::open(
                &MVCCDirectory::new(index_relation.oid(), need_wal),
                index_relation.oid(),
                req_receiver,
            ),
            BlockDirectoryType::BulkDelete => ChannelRequestHandler::open(
                &BulkDeleteDirectory::new(index_relation.oid()),
                index_relation.oid(),
                req_receiver,
            ),
        };

        let index = {
            let schema = schema.clone();
            handler
                .wait_for(move || {
                    let mut index = Index::open(channel_dir)?;
                    setup_tokenizers(&mut index, &schema);
                    tantivy::Result::Ok(index)
                })
                .expect("scoped thread should not fail")?
        };

        let writer = handler
            .wait_for(move || {
                let writer = index.writer_with_num_threads(parallelism.get(), memory_budget)?;
                writer.set_merge_policy(merge_policy.into());
                tantivy::Result::Ok(writer)
            })
            .expect("scoped thread should not fail")?;

        let ctid_field = schema.schema.get_field("ctid")?;
        Ok(Self {
            relation_oid: index_relation.oid(),
            writer: Arc::new(writer),
            schema,
            handler,
            wants_merge,
            ctid_field,
            insert_queue: Vec::with_capacity(MAX_INSERT_QUEUE_SIZE),
            need_wal,
        })
    }

    pub fn create_index(index_relation: &PgRelation) -> Result<Self> {
        let schema = get_index_schema(index_relation)?;
        let create_options = index_relation.rd_options as *mut SearchIndexCreateOptions;
        let (parallelism, memory_budget, wants_merge, merge_policy, need_wal) =
            WriterResources::CreateIndex.resources(unsafe { &*create_options });

        let (req_sender, req_receiver) = crossbeam::channel::bounded(CHANNEL_QUEUE_LEN);
        let channel_dir = ChannelDirectory::new(req_sender);
        let mut handler = ChannelRequestHandler::open(
            &MVCCDirectory::new(index_relation.oid(), need_wal),
            index_relation.oid(),
            req_receiver,
        );

        let index = {
            let schema = schema.clone();
            let settings = IndexSettings {
                docstore_compress_dedicated_thread: false,
                ..IndexSettings::default()
            };

            handler
                .wait_for(move || {
                    let mut index = Index::create(channel_dir, schema.schema.clone(), settings)?;
                    setup_tokenizers(&mut index, &schema);
                    tantivy::Result::Ok(index)
                })
                .expect("scoped thread should not fail")?
        };
        let writer = handler
            .wait_for(move || {
                let writer = index.writer_with_num_threads(parallelism.get(), memory_budget)?;
                writer.set_merge_policy(merge_policy.into());
                tantivy::Result::Ok(writer)
            })
            .expect("scoped thread should not fail")?;
        let ctid_field = schema.schema.get_field("ctid")?;

        Ok(Self {
            relation_oid: index_relation.oid(),
            writer: Arc::new(writer),
            schema,
            ctid_field,
            handler,
            wants_merge,
            insert_queue: Vec::with_capacity(MAX_INSERT_QUEUE_SIZE),
            need_wal,
        })
    }

    pub fn get_ctid_field(&self) -> Field {
        self.ctid_field
    }

    pub fn delete_term(&mut self, term: Term) -> Result<()> {
        self.insert_queue.push(UserOperation::Delete(term));
        if self.insert_queue.len() >= MAX_INSERT_QUEUE_SIZE {
            self.drain_insert_queue()?;
        }
        Ok(())
    }

    pub fn insert(&mut self, document: SearchDocument, ctid: Ctid) -> Result<()> {
        let mut tantivy_document: TantivyDocument = document.into();

        tantivy_document.add_u64(self.ctid_field, ((ctid.0 as u64) << 16) | ctid.1 as u64);

        self.insert_queue
            .push(UserOperation::AddWithCtid(tantivy_document, ctid));

        if self.insert_queue.len() >= MAX_INSERT_QUEUE_SIZE {
            self.drain_insert_queue()?;
        }
        Ok(())
    }

    pub fn commit(mut self, should_merge: bool) -> Result<()> {
        self.drain_insert_queue()?;
        let mut writer =
            Arc::into_inner(self.writer).expect("should not have an outstanding Arc<IndexWriter>");

        if should_merge {
            let _opstamp = self
                .handler
                .wait_for(move || {
                    let opstamp = writer.commit()?;
                    writer.wait_merging_threads()?;
                    tantivy::Result::Ok(opstamp)
                })
                .expect("spawned thread should not fail")?;
        } else {
            self.handler
                .wait_for(move || {
                    let policy: Box<dyn MergePolicy> = Box::new(NoMergePolicy);
                    writer.set_merge_policy(policy);
                    let opstamp = writer.commit()?;
                    tantivy::Result::Ok(opstamp)
                })
                .expect("spawned thread should not fail")?;
        };
        Ok(())
    }

    pub fn commit_inserts(self) -> Result<()> {
        let merge_lock = if self.wants_merge {
            unsafe { MergeLock::acquire_for_merge(self.relation_oid, self.need_wal) }
        } else {
            None
        };
        self.commit(merge_lock.is_some())
    }

    pub fn vacuum(self) -> Result<()> {
        assert!(self.insert_queue.is_empty());

        let merge_lock = unsafe { MergeLock::acquire_for_merge(self.relation_oid, self.need_wal) };
        self.commit(merge_lock.is_some())
    }

    fn drain_insert_queue(&mut self) -> Result<Opstamp, TantivyError> {
        let insert_queue = std::mem::take(&mut self.insert_queue);
        let writer = self.writer.clone();
        self.handler
            .wait_for(move || writer.run(insert_queue))
            .expect("spawned thread should not fail")
    }
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error(transparent)]
    TantivyError(#[from] tantivy::TantivyError),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    #[error(transparent)]
    TantivyValueError(#[from] TantivyValueError),

    #[error("key_field column '{0}' cannot be NULL")]
    KeyIdNull(String),
}
