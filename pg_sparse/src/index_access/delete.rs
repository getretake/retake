use pgrx::*;

use crate::sparse_index::SparseIndex;

#[pg_guard]
pub extern "C" fn ambulkdelete(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
    callback: pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut ::std::os::raw::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    let mut stats_binding = stats;

    if stats_binding.is_null() {
        stats_binding =
            unsafe { pg_sys::palloc0(std::mem::size_of::<pg_sys::IndexBulkDeleteResult>()).cast() };
    }

    let index_rel: pg_sys::Relation = unsafe { (*info).index };
    let index_relation = unsafe { PgRelation::from_pg(index_rel) };
    let index_name = index_relation.name().to_string();

    let sparse_index = SparseIndex::from_index_name(index_name);
    sparse_index.bulk_delete(stats_binding, callback, callback_state);
    stats_binding
}
