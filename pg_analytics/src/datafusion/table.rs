use async_std::stream::StreamExt;
use async_std::task;
use async_trait::async_trait;
use deltalake::datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
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
use crate::datafusion::datatype::{DatafusionTypeTranslator, PostgresTypeTranslator};
use crate::datafusion::directory::ParadeDirectory;
use crate::datafusion::writer::Writers;
use crate::errors::{NotFound, ParadeError};
use crate::guc::PARADE_GUC;

const BYTES_IN_MB: i64 = 1_048_576;

pub trait DatafusionTable {
    fn arrow_schema(&self) -> Result<Arc<ArrowSchema>, ParadeError>;
    fn table_path(&self) -> Result<PathBuf, ParadeError>;
}

impl DatafusionTable for PgRelation {
    fn arrow_schema(&self) -> Result<Arc<ArrowSchema>, ParadeError> {
        let tupdesc = self.tuple_desc();
        let mut fields = Vec::with_capacity(tupdesc.len());

        for attribute in tupdesc.iter() {
            if attribute.is_dropped() {
                continue;
            }

            let attname = attribute.name();
            let attribute_type_oid = attribute.type_oid();
            let nullability = !attribute.attnotnull;

            let array_type = unsafe { pg_sys::get_element_type(attribute_type_oid.value()) };
            let (base_oid, is_array) = if array_type != pg_sys::InvalidOid {
                (PgOid::from(array_type), true)
            } else {
                (attribute_type_oid, false)
            };

            // Note: even if you have an int[][], the attribute-type is INT4ARRAYOID and the base is INT4OID
            let field = if is_array {
                Field::new_list(
                    attname,
                    Field::new_list_field(
                        DataType::from_sql_data_type(
                            base_oid.to_sql_data_type(attribute.type_mod())?,
                        )?,
                        true, // TODO: i think postgres always allows array constants to be null
                    ),
                    nullability,
                )
            } else {
                Field::new(
                    attname,
                    DataType::from_sql_data_type(base_oid.to_sql_data_type(attribute.type_mod())?)?,
                    nullability,
                )
            };

            fields.push(field);
        }

        Ok(Arc::new(ArrowSchema::new(fields)))
    }

    fn table_path(&self) -> Result<PathBuf, ParadeError> {
        ParadeDirectory::table_path(
            DatafusionContext::catalog_oid()?,
            self.namespace_oid(),
            self.oid(),
        )
    }
}

pub struct Tables {
    tables: HashMap<PathBuf, DeltaTable>,
}

impl Tables {
    pub fn new() -> Result<Self, ParadeError> {
        Ok(Self {
            tables: HashMap::new(),
        })
    }

    pub async fn delete(
        &mut self,
        pg_relation: &PgRelation,
        predicate: Option<Expr>,
    ) -> Result<DeleteMetrics, ParadeError> {
        // Open the DeltaTable
        let table_name = pg_relation.name();
        let table_path = pg_relation.table_path()?;
        let old_table = Self::owned_table(self, table_path).await?;

        // Truncate the table
        let mut delete_builder = DeleteBuilder::new(
            old_table.log_store(),
            old_table
                .state
                .ok_or(NotFound::Value(type_name::<DeltaTableState>().to_string()))?,
        );

        if let Some(predicate) = predicate {
            delete_builder = delete_builder.with_predicate(predicate);
        }

        let (new_table, metrics) = delete_builder.await?;

        // Commit the table
        // Self::register_table(
        //     self,
        //     table_name.to_string(),
        //     Arc::new(new_table) as Arc<dyn TableProvider>,
        // )?;

        Ok(metrics)
    }

    pub async fn owned_table(&mut self, table_path: PathBuf) -> Result<DeltaTable, ParadeError> {
        let table = match Self::get_entry(self, table_path.clone())? {
            Occupied(entry) => entry.remove(),
            Vacant(entry) => deltalake::open_table(table_path.to_string_lossy()).await?,
        };

        Ok(table)
    }

    pub async fn vacuum(
        &mut self,
        pg_relation: &PgRelation,
        optimize: bool,
    ) -> Result<(), ParadeError> {
        let table_name = pg_relation.name();
        let table_path = pg_relation.table_path()?;
        let mut old_table = Self::owned_table(self, table_path).await?;

        if optimize {
            let optimized_table = OptimizeBuilder::new(
                old_table.log_store(),
                old_table
                    .state
                    .ok_or(NotFound::Value(type_name::<DeltaTableState>().to_string()))?,
            )
            .with_target_size(PARADE_GUC.optimize_file_size_mb.get() as i64 * BYTES_IN_MB)
            .await?
            .0;

            old_table = optimized_table;
        }

        let vacuumed_table = VacuumBuilder::new(
            old_table.log_store(),
            old_table
                .state
                .ok_or(NotFound::Value(type_name::<DeltaTableState>().to_string()))?,
        )
        .with_retention_period(chrono::Duration::days(
            PARADE_GUC.vacuum_retention_days.get() as i64,
        ))
        .with_enforce_retention_duration(PARADE_GUC.vacuum_enforce_retention.get())
        .await?
        .0;

        // Commit the vacuumed table
        // Self::register_table(
        //     self,
        //     table_name.to_string(),
        //     Arc::new(vacuumed_table) as Arc<dyn TableProvider>,
        // )?;

        Ok(())
    }

    pub async fn vacuum_all(
        &mut self,
        schema_path: PathBuf,
        optimize: bool,
    ) -> Result<(), ParadeError> {
        let directory = std::fs::read_dir(schema_path.clone())?;

        // Vacuum all tables in the schema directory and delete directories for dropped tables
        for file in directory {
            let table_oid = file?.file_name().into_string()?;

            if let Ok(oid) = table_oid.parse::<u32>() {
                let pg_oid = pg_sys::Oid::from(oid);
                let relation = unsafe { pg_sys::RelationIdGetRelation(pg_oid) };

                // If the relation is null, delete the directory
                if relation.is_null() {
                    let path = schema_path.join(&table_oid);
                    remove_dir_all(path.clone())?;
                // Otherwise, vacuum the table
                } else {
                    let pg_relation = unsafe { PgRelation::from_pg(relation) };
                    Self::vacuum(self, &pg_relation, optimize).await?;
                    unsafe { pg_sys::RelationClose(relation) }
                }
            }
        }

        Ok(())
    }

    fn get_entry(&mut self, table_path: PathBuf) -> Result<Entry<PathBuf, DeltaTable>, ParadeError> {
        Ok(self.tables.entry(table_path))
    }

    async fn create_table(pg_relation: &PgRelation) -> Result<DeltaTable, ParadeError> {
        let table_path = pg_relation.table_path()?;
        let table_name = pg_relation.name();
        let schema_oid = pg_relation.namespace_oid();
        let arrow_schema = pg_relation.arrow_schema()?;
        let delta_schema = DeltaSchema::try_from(arrow_schema.as_ref())?;
        let batch = RecordBatch::new_empty(arrow_schema.clone());

        ParadeDirectory::create_schema_path(DatafusionContext::catalog_oid()?, schema_oid)?;

        let mut delta_table = CreateBuilder::new()
            .with_location(table_path.to_string_lossy())
            .with_columns(delta_schema.fields().to_vec())
            .await?;

        // Write the RecordBatch to the DeltaTable
        let writers = DatafusionContext::with_schema_provider(pg_relation.namespace(), |provider| {
            provider.writers()
        })?;

        writers.lock().merge_schema(table_name, table_path, batch).await?;

        // Update the DeltaTable
        delta_table.update().await?;

        Ok(delta_table)
    }
}
