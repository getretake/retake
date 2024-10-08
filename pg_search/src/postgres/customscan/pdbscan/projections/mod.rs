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

pub mod score;
pub mod snippet;

use crate::nodecast;
use crate::postgres::customscan::pdbscan::projections::score::score_funcoid;
use crate::postgres::customscan::pdbscan::projections::snippet::snippet_funcoid;
use pgrx::pg_sys::expression_tree_walker;
use pgrx::{pg_guard, pg_sys, PgList};
use std::ptr::addr_of_mut;

pub unsafe fn maybe_needs_const_projections(node: *mut pg_sys::Node) -> bool {
    #[pg_guard]
    unsafe extern "C" fn walker(node: *mut pg_sys::Node, data: *mut core::ffi::c_void) -> bool {
        if node.is_null() {
            return false;
        }

        if let Some(funcexpr) = nodecast!(FuncExpr, T_FuncExpr, node) {
            let data = &*data.cast::<Data>();
            if (*funcexpr).funcid == data.score_funcoid
                || (*funcexpr).funcid == data.snipped_funcoid
            {
                return true;
            }
        }

        expression_tree_walker(node, Some(walker), data)
    }

    struct Data {
        score_funcoid: pg_sys::Oid,
        snipped_funcoid: pg_sys::Oid,
    }

    let mut data = Data {
        score_funcoid: score_funcoid(),
        snipped_funcoid: snippet_funcoid(),
    };

    let data = addr_of_mut!(data).cast();
    walker(node, data)
}

/// find all [`pg_sys::FuncExpr`] nodes matching a set of known function Oids that also contain
/// a [`pg_sys::Var`] as an argument that the specified `rti` level.
///
/// Returns a [`Vec`] of the matching `FuncExpr`s and the argument `Var` that finally matched.  If
/// the function has multiple arguments that match, it's returned multiple times.
pub unsafe fn pullout_funcexprs(
    node: *mut pg_sys::Node,
    funcids: &[pg_sys::Oid],
    rti: i32,
) -> Vec<(*mut pg_sys::FuncExpr, *mut pg_sys::Var)> {
    #[pg_guard]
    unsafe extern "C" fn walker(node: *mut pg_sys::Node, data: *mut core::ffi::c_void) -> bool {
        if node.is_null() {
            return false;
        }

        if let Some(funcexpr) = nodecast!(FuncExpr, T_FuncExpr, node) {
            let data = &mut *data.cast::<Data>();
            if data.funcids.contains(&(*funcexpr).funcid) {
                let args = PgList::<pg_sys::Node>::from_pg((*funcexpr).args);
                for arg in args.iter_ptr() {
                    if let Some(var) = nodecast!(Var, T_Var, arg) {
                        if (*var).varno as i32 == data.rti as i32 {
                            data.matches.push((funcexpr, var));
                        }
                    }
                }

                return false;
            }
        }

        expression_tree_walker(node, Some(walker), data)
    }

    struct Data<'a> {
        funcids: &'a [pg_sys::Oid],
        rti: i32,
        matches: Vec<(*mut pg_sys::FuncExpr, *mut pg_sys::Var)>,
    }

    let mut data = Data {
        funcids,
        rti,
        matches: vec![],
    };

    walker(node, addr_of_mut!(data).cast());
    data.matches
}
