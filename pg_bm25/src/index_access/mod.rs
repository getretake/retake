use pgrx::*;

mod build;
mod cost;
mod delete;
mod insert;
pub mod options;
mod scan;
mod vacuum;
mod validate;

pub mod utils;

#[pg_extern(sql = "
CREATE FUNCTION bm25_handler(internal) RETURNS index_am_handler PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
CREATE ACCESS METHOD bm25 TYPE INDEX HANDLER bm25_handler;
COMMENT ON ACCESS METHOD bm25 IS 'bm25 index access method';
")]
fn bm25_handler(_fcinfo: pg_sys::FunctionCallInfo) -> PgBox<pg_sys::IndexAmRoutine> {
    let mut amroutine =
        unsafe { PgBox::<pg_sys::IndexAmRoutine>::alloc_node(pg_sys::NodeTag::T_IndexAmRoutine) };

    amroutine.amstrategies = 4;
    amroutine.amsupport = 0;
    amroutine.amcanmulticol = true;
    amroutine.amsearcharray = true;

    amroutine.amkeytype = pg_sys::InvalidOid;

    amroutine.amvalidate = Some(validate::amvalidate);
    amroutine.ambuild = Some(build::ambuild);
    amroutine.ambuildempty = Some(build::ambuildempty);
    amroutine.aminsert = Some(insert::aminsert);
    amroutine.ambulkdelete = Some(delete::ambulkdelete);
    amroutine.amvacuumcleanup = Some(vacuum::amvacuumcleanup);
    amroutine.amcostestimate = Some(cost::amcostestimate);
    amroutine.amoptions = Some(options::amoptions);
    amroutine.ambeginscan = Some(scan::ambeginscan);
    amroutine.amrescan = Some(scan::amrescan);
    amroutine.amgettuple = Some(scan::amgettuple);
    amroutine.amgetbitmap = Some(scan::ambitmapscan);
    amroutine.amendscan = Some(scan::amendscan);

    amroutine.into_pg_boxed()
}
