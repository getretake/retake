use async_std::stream::StreamExt;
use async_std::task;
use async_trait::async_trait;
use deltalake::datafusion::arrow::datatypes::Schema as ArrowSchema;
use deltalake::datafusion::arrow::record_batch::RecordBatch;
use deltalake::datafusion::catalog::schema::SchemaProvider;
use deltalake::datafusion::datasource::TableProvider;
use deltalake::datafusion::error::Result;
use deltalake::datafusion::execution::context::SessionState;
use deltalake::datafusion::execution::TaskContext;
use deltalake::datafusion::logical_expr::Expr;
use deltalake::datafusion::physical_plan::SendableRecordBatchStream;
use deltalake::kernel::Schema as DeltaSchema;
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::delete::{DeleteBuilder, DeleteMetrics};
use deltalake::operations::optimize::OptimizeBuilder;
use deltalake::operations::update::UpdateBuilder;
use deltalake::operations::vacuum::VacuumBuilder;
use deltalake::table::state::DeltaTableState;
use deltalake::writer::DeltaWriter as DeltaWriterTrait;
use deltalake::DeltaTable;
use parking_lot::{Mutex, RwLock};
use pgrx::*;
use std::collections::{
    hash_map::Entry::{self, Occupied, Vacant},
    HashMap,
};
use std::future::IntoFuture;
use std::{
    any::type_name, any::Any, ffi::CStr, ffi::CString, fs::remove_dir_all, path::PathBuf, sync::Arc,
};

use crate::datafusion::context::DatafusionContext;
use crate::datafusion::directory::ParadeDirectory;
use crate::datafusion::table::{DatafusionTable, Tables};
use crate::datafusion::writer::Writers;
use crate::errors::{NotFound, ParadeError};
use crate::guc::PARADE_GUC;

const BYTES_IN_MB: i64 = 1_048_576;

pub struct ParadeSchemaProvider {
    schema_name: String,
    tables: Arc<Mutex<Tables>>,
    writers: Arc<Mutex<Writers>>,
    streams: Mutex<HashMap<String, SendableRecordBatchStream>>,
    dir: PathBuf,
}

impl ParadeSchemaProvider {
    pub async fn try_new(schema_name: &str, dir: PathBuf) -> Result<Self, ParadeError> {
        Ok(Self {
            schema_name: schema_name.to_string(),
            tables: Arc::new(Mutex::new(Tables::new()?)),
            writers: Arc::new(Mutex::new(Writers::new(schema_name)?)),
            streams: Mutex::new(HashMap::new()),
            dir,
        })
    }

    pub fn tables(&self) -> Result<Arc<Mutex<Tables>>, ParadeError> {
        Ok(self.tables.clone())
    }

    pub fn writers(&self) -> Result<Arc<Mutex<Writers>>, ParadeError> {
        Ok(self.writers.clone())
    }

    pub fn register_stream(
        &self,
        name: &str,
        stream: SendableRecordBatchStream,
    ) -> Result<(), ParadeError> {
        let mut streams = self.streams.lock();
        streams.insert(name.to_string(), stream);

        Ok(())
    }

    pub async fn create_stream(
        &mut self,
        name: &str,
        state: &SessionState,
        task_context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, ParadeError> {
        let delta_table = self.tables.lock().owned_table("".into()).await?;

        Ok(delta_table
            .scan(state, None, &[], None)
            .await
            .map(|plan| plan.execute(0, task_context))??)
    }

    pub fn get_next_streamed_batch(&self, name: &str) -> Result<Option<RecordBatch>, ParadeError> {
        let mut streams = self.streams.lock();
        let stream = streams
            .get_mut(name)
            .ok_or(NotFound::Stream(name.to_string()))?;

        let batch = task::block_on(stream.next());

        match batch {
            Some(Ok(b)) => Ok(Some(b)),
            None => {
                streams.remove(name);
                Ok(None)
            }
            Some(Err(err)) => Err(ParadeError::DataFusion(err)),
        }
    }
}

#[async_trait]
impl SchemaProvider for ParadeSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // self.tables
        //     .read()
        //     .values()
        //     .map(|table| table.table_name)
        //     .collect()
        vec![]
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        // let table_path = ParadeDirectory::table_path(
        //     DatafusionContext::catalog_oid().ok()?,
        //     self.schema_name.clone(),
        //     name.to_string(),
        // )
        // .ok();

        // let tables = self.tables.read();

        // match tables.get_table(table_path)? {
        //     Occupied(entry) => Some(Arc::new(entry.into_mut()) as Arc<dyn TableProvider>),
        //     Vacant(entry) => {
        //         // TODO register table
        //         Some(entry.insert(tables.create_table(table_path).ok()?).ok())
        //     }
        // }
        None
    }

    fn table_exist(&self, name: &str) -> bool {
        // let table_path = ParadeDirectory::table_path(
        //     DatafusionContext::catalog_oid().ok()?,
        //     self.schema_name.clone(),
        //     name.to_string(),
        // )
        // .ok();

        // let tables = self.tables.read();
        // tables.contains_key(table_path)
        false
    }

    // fn register_table(
    //     &self,
    //     name: String,
    //     table: Arc<dyn TableProvider>,
    // ) -> Result<Option<Arc<dyn TableProvider>>> {
    //     let mut tables = self.tables.write();
    //     let table_path = ParadeDirectory::table_path(
    //         DatafusionContext::catalog_oid().ok()?,
    //         self.schema_name.clone(),
    //         name,
    //     ).ok()?;

    //     tables.insert(table_path, table.clone());
    //     Ok(Some(table))
    // }

    // fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
    //     let mut tables = self.tables.write();
    //     Ok(tables.remove(name))
    // }
}
