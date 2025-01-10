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
use pgrx::PgRelation;
use std::sync::Arc;
use tantivy::indexer::UserOperation;
use tantivy::schema::Field;
use tantivy::{
    Ctid, Index, IndexSettings, IndexWriter, Opstamp, TantivyDocument, TantivyError, Term,
};
use thiserror::Error;

use crate::index::channel::{ChannelDirectory, ChannelRequestHandler};
use crate::index::{get_index_schema, setup_tokenizers, BlockDirectoryType, WriterResources};
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
    insert_queue: Vec<UserOperation>,
}

impl SearchIndexWriter {
    pub fn open(
        index_relation: &PgRelation,
        directory_type: BlockDirectoryType,
        resources: WriterResources,
    ) -> Result<Self> {
        let (parallelism, memory_budget, merge_policy) = resources.resources(index_relation);

        let (req_sender, req_receiver) = crossbeam::channel::bounded(CHANNEL_QUEUE_LEN);
        let channel_dir = ChannelDirectory::new(req_sender);
        let mut handler =
            directory_type.channel_request_handler(index_relation, req_receiver, merge_policy);

        let mut index = {
            handler
                .wait_for(move || {
                    let index = Index::open(channel_dir)?;
                    tantivy::Result::Ok(index)
                })
                .expect("scoped thread should not fail")?
        };
        setup_tokenizers(&mut index, index_relation);

        let index_clone = index.clone();
        let writer = handler
            .wait_for(move || {
                let writer =
                    index_clone.writer_with_num_threads(parallelism.get(), memory_budget)?;
                tantivy::Result::Ok(writer)
            })
            .expect("scoped thread should not fail")?;

        let schema = SearchIndexSchema::open(index.schema(), index_relation);
        let ctid_field = schema.schema.get_field("ctid")?;

        Ok(Self {
            writer: Arc::new(writer),
            schema,
            handler,
            ctid_field,
            insert_queue: Vec::with_capacity(MAX_INSERT_QUEUE_SIZE),
        })
    }

    pub fn create_index(index_relation: &PgRelation) -> Result<Self> {
        let schema = get_index_schema(index_relation)?;
        let (parallelism, memory_budget, merge_policy) =
            WriterResources::CreateIndex.resources(index_relation);

        let (req_sender, req_receiver) = crossbeam::channel::bounded(CHANNEL_QUEUE_LEN);
        let channel_dir = ChannelDirectory::new(req_sender);
        let mut handler = BlockDirectoryType::Mvcc.channel_request_handler(
            index_relation,
            req_receiver,
            merge_policy,
        );

        let mut index = {
            let schema = schema.clone();
            let settings = IndexSettings {
                docstore_compress_dedicated_thread: false,
                ..IndexSettings::default()
            };

            handler
                .wait_for(move || {
                    let index = Index::create(channel_dir, schema.schema.clone(), settings)?;
                    tantivy::Result::Ok(index)
                })
                .expect("scoped thread should not fail")?
        };
        setup_tokenizers(&mut index, index_relation);

        let writer = handler
            .wait_for(move || {
                let writer = index.writer_with_num_threads(parallelism.get(), memory_budget)?;
                tantivy::Result::Ok(writer)
            })
            .expect("scoped thread should not fail")?;
        let ctid_field = schema.schema.get_field("ctid")?;

        Ok(Self {
            writer: Arc::new(writer),
            schema,
            ctid_field,
            handler,
            insert_queue: Vec::with_capacity(MAX_INSERT_QUEUE_SIZE),
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

    pub fn commit(mut self) -> Result<()> {
        self.drain_insert_queue()?;
        let mut writer =
            Arc::into_inner(self.writer).expect("should not have an outstanding Arc<IndexWriter>");

        self.handler
            .wait_for(move || {
                let opstamp = writer.commit()?;
                writer.wait_merging_threads()?;
                tantivy::Result::Ok(opstamp)
            })
            .expect("spawned thread should not fail")?;

        // TODO: Bring this back
        // unsafe {
        //     garbage_collect_metas(self.relation_oid)?;
        // }
        Ok(())
    }

    pub fn vacuum(self) -> Result<()> {
        assert!(self.insert_queue.is_empty());
        self.commit()
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
