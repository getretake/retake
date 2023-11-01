use pgrx::*;

use crate::index_access::utils::{
    categorize_tupdesc, get_parade_index, lookup_index_tupdesc, row_to_json,
};

#[allow(clippy::too_many_arguments)]
#[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16"))]
#[pg_guard]
pub unsafe extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck,
    _index_unchanged: bool,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    aminsert_internal(index_relation, values, heap_tid)
}

#[cfg(any(feature = "pg11", feature = "pg12", feature = "pg13"))]
#[pg_guard]
pub unsafe extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    aminsert_internal(index_relation, values, heap_tid)
}

#[inline(always)]
unsafe fn aminsert_internal(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    heap_tid: pg_sys::ItemPointer,
) -> bool {
    let index_relation_ref: PgRelation = PgRelation::from_pg(index_relation);
    let index_name = index_relation_ref.name().to_string();

    let tupdesc = lookup_index_tupdesc(&index_relation_ref);
    let attributes = categorize_tupdesc(&tupdesc);
    let natts = tupdesc.natts as usize;
    let dropped = (0..tupdesc.natts as usize)
        .map(|i| tupdesc.get(i).unwrap().is_dropped())
        .collect::<Vec<bool>>();
    let values = std::slice::from_raw_parts(values, 1);
    let builder = row_to_json(values[0], &tupdesc, natts, &dropped, &attributes);

    // Insert row to parade index
    let mut parade_index = get_parade_index(index_name);
    parade_index.insert(*heap_tid, builder);

    true
}
