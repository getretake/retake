use pgrx::*;
use std::ptr::null_mut;

use crate::sparse_index::index::from_index_name;

#[allow(clippy::too_many_arguments)]
#[pg_guard(immutable, parallel_safe)]
pub unsafe extern "C" fn amcostestimate(
    root: *mut pg_sys::PlannerInfo,
    path: *mut pg_sys::IndexPath,
    loop_count: f64,
    index_startup_cost: *mut pg_sys::Cost,
    index_total_cost: *mut pg_sys::Cost,
    index_selectivity: *mut pg_sys::Selectivity,
    index_correlation: *mut f64,
    index_pages: *mut f64,
) {
    let pathref = path.as_ref().expect("path argument is NULL");

    if pathref.indexorderbys == null_mut() {
        *index_startup_cost = f64::MAX;
        *index_total_cost = f64::MAX;
        *index_selectivity = 0.0;
        *index_correlation = 0.0;
        *index_pages = 0.0;
    } else {
        let indexinfo = pathref
            .indexinfo
            .as_ref()
            .expect("indexinfo in path is NULL");
        let index_relation = unsafe {
            PgRelation::with_lock(
                indexinfo.indexoid,
                pg_sys::AccessShareLock as pg_sys::LOCKMODE,
            )
        };
        let heap_relation = index_relation
            .heap_relation()
            .expect("failed to get heap relation for index");
        let mut sparse_index = from_index_name(index_relation.name());
        let meta = sparse_index.get_hnsw_metadata();
        let ef_search = meta.ef_search as f64;

        let mut generic_costs = pg_sys::GenericCosts::default();
        pg_sys::genericcostestimate(root, path, loop_count, &mut generic_costs);

        *index_startup_cost = ef_search * pg_sys::random_page_cost;
        *index_total_cost = *index_startup_cost;
        *index_selectivity = if (*indexinfo.rel).rows != 0.0 {
            ef_search / (*indexinfo.rel).rows
        } else {
            generic_costs.indexSelectivity
        };
        *index_correlation = generic_costs.indexCorrelation;
        *index_pages = ef_search;
    }
}
