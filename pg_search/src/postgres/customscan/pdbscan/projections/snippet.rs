use crate::api::index::FieldName;
use crate::index::state::SearchState;
use crate::nodecast;
use crate::postgres::customscan::pdbscan::projections::OpaqueRecordArg;
use pgrx::pg_sys::expression_tree_walker;
use pgrx::{
    default, direct_function_call, pg_extern, pg_guard, pg_sys, FromDatum, IntoDatum, PgList,
};
use std::ptr::addr_of_mut;
use tantivy::snippet::SnippetGenerator;
use tantivy::DocAddress;

#[pg_extern(name = "snippet", stable, parallel_safe)]
fn snippet_from_relation(
    _relation_reference: OpaqueRecordArg,
    field: FieldName,
    start_tag: default!(String, "'<b>'"),
    end_tag: default!(String, "'</b>'"),
) -> String {
    format!("<could not generate snippet for {}>", field)
}

pub fn snippet_funcoid() -> pg_sys::Oid {
    unsafe {
        direct_function_call::<pg_sys::Oid>(
            pg_sys::regprocedurein,
            &[c"paradedb.snippet(record, paradedb.fieldname, text, text)".into_datum()],
        )
        .expect("the `paradedb.snippet(record, paradedb.fieldname, text, text) type should exist")
    }
}

pub unsafe fn uses_snippets(
    node: *mut pg_sys::Node,
    snippet_funcoid: pg_sys::Oid,
) -> Vec<(FieldName, Option<String>, Option<String>)> {
    struct Context {
        snippet_funcoid: pg_sys::Oid,
        fieldnames: Vec<(FieldName, Option<String>, Option<String>)>,
    }

    #[pg_guard]
    unsafe extern "C" fn walker(node: *mut pg_sys::Node, data: *mut core::ffi::c_void) -> bool {
        if node.is_null() {
            return false;
        }

        if let Some(funcexpr) = nodecast!(FuncExpr, T_FuncExpr, node) {
            let context = data.cast::<Context>();
            let args = PgList::<pg_sys::Node>::from_pg((*funcexpr).args);

            if (*funcexpr).funcid == (*context).snippet_funcoid {
                assert!(args.len() == 4);

                let field_arg = nodecast!(Const, T_Const, args.get_ptr(1).unwrap());
                let start_arg = nodecast!(Const, T_Const, args.get_ptr(2).unwrap());
                let end_arg = nodecast!(Const, T_Const, args.get_ptr(3).unwrap());

                if let (Some(field_arg), Some(start_arg), Some(end_arg)) =
                    (field_arg, start_arg, end_arg)
                {
                    let field =
                        FieldName::from_datum((*field_arg).constvalue, (*field_arg).constisnull)
                            .expect("`paradedb.snippet()`'s field argument cannot be NULL");
                    let start =
                        String::from_datum((*start_arg).constvalue, (*start_arg).constisnull);
                    let end = String::from_datum((*end_arg).constvalue, (*end_arg).constisnull);

                    (*context).fieldnames.push((field, start, end));
                } else {
                    panic!("`paradedb.snippet()`'s field and (optional) tag arguments must be text literals")
                }
            }
        }

        expression_tree_walker(node, Some(walker), data)
    }

    let mut context = Context {
        snippet_funcoid,
        fieldnames: vec![],
    };

    walker(node, addr_of_mut!(context).cast());
    context.fieldnames
}

#[allow(clippy::too_many_arguments)]
pub unsafe fn inject_snippet(
    node: *mut pg_sys::Node,
    snippet_funcoid: pg_sys::Oid,
    search_state: &SearchState,
    field: &FieldName,
    start: &str,
    end: &str,
    snippet_generator: &SnippetGenerator,
    doc_address: DocAddress,
) -> *mut pg_sys::Node {
    struct Context<'a> {
        snippet_funcoid: pg_sys::Oid,
        search_state: &'a SearchState,
        field: &'a FieldName,
        start: &'a str,
        end: &'a str,
        snippet_generator: &'a SnippetGenerator,
        doc_address: DocAddress,
    }

    #[pg_guard]
    unsafe extern "C" fn walker(
        node: *mut pg_sys::Node,
        data: *mut core::ffi::c_void,
    ) -> *mut pg_sys::Node {
        if node.is_null() {
            return std::ptr::null_mut();
        }

        if let Some(funcexpr) = nodecast!(FuncExpr, T_FuncExpr, node) {
            let context = data.cast::<Context>();
            let args = PgList::<pg_sys::Node>::from_pg((*funcexpr).args);

            if (*funcexpr).funcid == (*context).snippet_funcoid {
                assert!(args.len() == 4);

                if let Some(second_arg) = nodecast!(Const, T_Const, args.get_ptr(1).unwrap()) {
                    let fieldname =
                        FieldName::from_datum((*second_arg).constvalue, (*second_arg).constisnull)
                            .expect("`paradedb.snippet()`'s field argument cannot be NULL");

                    if &fieldname == (*context).field {
                        let doc = (*context)
                            .search_state
                            .get_doc((*context).doc_address)
                            .expect("should be able to retrieve doc for snippet generation");

                        let mut snippet = (*context).snippet_generator.snippet_from_doc(&doc);
                        snippet.set_snippet_prefix_postfix((*context).start, (*context).end);

                        let html = snippet.to_html().into_datum().unwrap();
                        let const_ = pg_sys::makeConst(
                            pg_sys::TEXTOID,
                            -1,
                            pg_sys::DEFAULT_COLLATION_OID,
                            -1,
                            html,
                            false,
                            false,
                        );
                        return const_.cast();
                    }
                }
            }
        }

        pg_sys::expression_tree_mutator_impl(node, Some(walker), data)
    }

    let mut context = Context {
        snippet_funcoid,
        search_state,
        field,
        start,
        end,
        snippet_generator,
        doc_address,
    };

    let data = addr_of_mut!(context);
    walker(node, data.cast())
}
