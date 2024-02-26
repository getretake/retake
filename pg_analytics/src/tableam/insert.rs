use async_std::task;
use core::ffi::c_int;
use deltalake::datafusion::arrow::record_batch::RecordBatch;
use deltalake::datafusion::common::arrow::array::ArrayRef;
use pgrx::*;

use crate::datafusion::context::DatafusionContext;
use crate::datafusion::datatype::DatafusionMapProducer;
use crate::datafusion::datatype::DatafusionTypeTranslator;
use crate::datafusion::datatype::PostgresTypeTranslator;
use crate::datafusion::directory::ParadeDirectory;
use crate::datafusion::table::DatafusionTable;
use crate::errors::ParadeError;

#[pg_guard]
pub extern "C" fn deltalake_slot_callbacks(
    _rel: pg_sys::Relation,
) -> *const pg_sys::TupleTableSlotOps {
    unsafe { &pg_sys::TTSOpsVirtual }
}

#[pg_guard]
pub extern "C" fn deltalake_tuple_insert(
    rel: pg_sys::Relation,
    slot: *mut pg_sys::TupleTableSlot,
    _cid: pg_sys::CommandId,
    _options: c_int,
    _bistate: *mut pg_sys::BulkInsertStateData,
) {
    let mut mut_slot = slot;
    insert_tuples(rel, &mut mut_slot, 1).unwrap_or_else(|err| {
        panic!("{}", err);
    });
}

#[pg_guard]
pub extern "C" fn deltalake_multi_insert(
    rel: pg_sys::Relation,
    slots: *mut *mut pg_sys::TupleTableSlot,
    nslots: c_int,
    _cid: pg_sys::CommandId,
    _options: c_int,
    _bistate: *mut pg_sys::BulkInsertStateData,
) {
    insert_tuples(rel, slots, nslots as usize).unwrap_or_else(|err| {
        panic!("{}", err);
    });
}

#[pg_guard]
pub extern "C" fn deltalake_finish_bulk_insert(rel: pg_sys::Relation, _options: c_int) {
    flush_and_commit(rel).unwrap_or_else(|err| {
        panic!("{}", err);
    });
}

#[pg_guard]
pub extern "C" fn deltalake_tuple_insert_speculative(
    _rel: pg_sys::Relation,
    _slot: *mut pg_sys::TupleTableSlot,
    _cid: pg_sys::CommandId,
    _options: c_int,
    _bistate: *mut pg_sys::BulkInsertStateData,
    _specToken: pg_sys::uint32,
) {
}

#[inline]
fn flush_and_commit(rel: pg_sys::Relation) -> Result<(), ParadeError> {
    let pg_relation = unsafe { PgRelation::from_pg(rel) };
    let schema_name = pg_relation.namespace();
    let table_oid = pg_relation.oid();
    let schema_oid = pg_relation.namespace_oid();
    let table_path =
        ParadeDirectory::table_path(DatafusionContext::catalog_oid()?, schema_oid, table_oid)?;

    let writer_lock =
        DatafusionContext::with_schema_provider(schema_name, |provider| provider.writers())?;

    let mut writer = writer_lock.lock();
    task::block_on(writer.flush_and_commit(pg_relation.name(), table_path))?;

    Ok(())
}

#[inline]
fn insert_tuples(
    rel: pg_sys::Relation,
    slots: *mut *mut pg_sys::TupleTableSlot,
    nslots: usize,
) -> Result<(), ParadeError> {
    let pg_relation = unsafe { PgRelation::from_pg(rel) };
    let tuple_desc = pg_relation.tuple_desc();
    let mut values: Vec<ArrayRef> = vec![];

    // Convert the TupleTableSlots into DataFusion arrays
    for (col_idx, attr) in tuple_desc.iter().enumerate() {
        let sql_data_type = attr.type_oid().to_sql_data_type(attr.type_mod())?;
        let datafusion_type = DatafusionTypeTranslator::from_sql_data_type(sql_data_type)?;

        values.push(DatafusionMapProducer::array(
            datafusion_type,
            slots,
            nslots,
            col_idx,
        )?);
    }

    // Create a RecordBatch
    let pg_relation = unsafe { PgRelation::from_pg(rel) };
    let schema_name = pg_relation.namespace();
    let table_oid = pg_relation.oid();
    let schema_oid = pg_relation.namespace_oid();
    let table_path =
        ParadeDirectory::table_path(DatafusionContext::catalog_oid()?, schema_oid, table_oid)?;
    let arrow_schema = pg_relation.arrow_schema()?;
    let batch = RecordBatch::try_new(arrow_schema.clone(), values)?;

    let writer_lock =
        DatafusionContext::with_schema_provider(schema_name, |provider| provider.writers())?;
    let mut writer = writer_lock.lock();

    // Write the RecordBatch to the Delta table
    task::block_on(writer.write(&pg_relation, batch))?;

    Ok(())
}
