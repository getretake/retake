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

use crate::index::reader::index::{SearchIndexReader, SearchResults};
use crate::postgres::customscan::builders::custom_path::SortDirection;
use crate::postgres::customscan::pdbscan::exec_methods::{ExecMethod, ExecState};
use crate::postgres::customscan::pdbscan::parallel::checkout_segment;
use crate::postgres::customscan::pdbscan::scan_state::PdbScanState;
use crate::query::SearchQueryInput;
use pgrx::{direct_function_call, pg_sys, IntoDatum};
use tantivy::SegmentOrdinal;

// TODO:  should these be GUCs?  I think yes, probably
const SUBSEQUENT_RETRY_SCALE_FACTOR: usize = 2;
const MAX_CHUNK_SIZE: usize = 5000;

#[derive(Default)]
pub struct TopNScanExecState {
    // required
    heaprelid: pg_sys::Oid,
    limit: usize,
    sort_direction: SortDirection,

    // set during init
    have_less: bool,
    search_query_input: Option<SearchQueryInput>,
    search_reader: Option<SearchIndexReader>,
    sort_field: Option<String>,
    search_results: SearchResults,
    did_query: bool,

    // state tracking
    last_ctid: u64,
    found: usize,
    chunk_size: usize,
    retry_count: usize,
    current_segment: SegmentOrdinal,
}

impl TopNScanExecState {
    pub fn new(heaprelid: pg_sys::Oid, limit: usize, sort_direction: SortDirection) -> Self {
        Self {
            heaprelid,
            limit,
            sort_direction,
            ..Default::default()
        }
    }

    fn query_more_results(
        &mut self,
        state: &mut PdbScanState,
        current_segment: Option<SegmentOrdinal>,
    ) -> SearchResults {
        if let Some(parallel_state) = state.parallel_state {
            // we're parallel, so either query the provided segment or go get a segment from the parallel state
            let segment_ord = current_segment
                .map(Some)
                .unwrap_or_else(|| unsafe { checkout_segment(parallel_state) });

            if let Some(segment_ord) = segment_ord {
                self.current_segment = segment_ord;

                let search_reader = state.search_reader.as_ref().unwrap();
                search_reader.search_top_n_in_segment(
                    segment_ord,
                    self.search_query_input.as_ref().unwrap(),
                    self.sort_field.clone(),
                    self.sort_direction.into(),
                    self.limit,
                )
            } else {
                // no more segments to query
                SearchResults::None
            }
        } else if self.did_query {
            // not parallel, so we're done
            SearchResults::None
        } else {
            // not parallel, first time query
            let search_reader = state.search_reader.as_ref().unwrap();
            search_reader.search_top_n(
                self.search_query_input.as_ref().unwrap(),
                self.sort_field.clone(),
                self.sort_direction.into(),
                self.limit,
            )
        }
    }

    fn reset(&mut self) {
        self.found = 0;
        self.last_ctid = 0;
        self.chunk_size = 0;
        self.retry_count = 0;
        self.have_less = false;
    }
}

impl ExecMethod for TopNScanExecState {
    fn init(&mut self, state: &mut PdbScanState, _cstate: *mut pg_sys::CustomScanState) {
        let sort_field = state.sort_field.clone();

        self.search_query_input = Some(state.search_query_input.clone());
        self.sort_field = sort_field;
        self.search_reader = state.search_reader.clone();
    }

    fn query(&mut self, state: &mut PdbScanState) -> bool {
        let search_results = self.query_more_results(state, None);

        self.did_query = true;

        if matches!(search_results, SearchResults::None) {
            false
        } else {
            let len = search_results
                .len()
                .expect("search_results should not be empty");
            self.have_less = len < self.limit;
            self.search_results = search_results;
            self.reset();
            true
        }
    }

    fn internal_next(&mut self, state: &mut PdbScanState) -> ExecState {
        unsafe {
            let mut next = self.search_results.next();
            loop {
                match next {
                    None if !self.did_query => {
                        // we haven't even done a query yet, so this is our very first time in
                        return ExecState::Eof;
                    }
                    None => {
                        if self.found == self.limit || self.have_less {
                            // we found all the matching rows
                            return ExecState::Eof;
                        }
                    }
                    Some((scored, doc_address)) => {
                        return ExecState::RequiresVisibilityCheck {
                            ctid: scored.ctid,
                            score: scored.bm25,
                            doc_address,
                        };
                    }
                }

                // we underflowed our tuples, so go get some more, if there are any
                self.retry_count += 1;

                // calculate a scaling factor to use against the limit
                let factor = if self.chunk_size == 0 {
                    // if we haven't done any chunking yet, calculate the scaling factor
                    // based on the proportion of dead tuples compared to live tuples
                    let heaprelid = self.heaprelid;
                    let n_dead = direct_function_call::<i64>(
                        pg_sys::pg_stat_get_dead_tuples,
                        &[heaprelid.into_datum()],
                    )
                    .unwrap();
                    let n_live = direct_function_call::<i64>(
                        pg_sys::pg_stat_get_live_tuples,
                        &[heaprelid.into_datum()],
                    )
                    .unwrap();

                    (1.0 + ((1.0 + n_dead as f64) / (1.0 + n_live as f64))).ceil() as usize
                } else {
                    // we've already done chunking, so just use a default scaling factor
                    // to avoid exponentially growing the chunk size
                    SUBSEQUENT_RETRY_SCALE_FACTOR
                };

                // set the chunk size to the scaling factor times the limit
                self.chunk_size = (self.chunk_size * factor)
                    .max(self.limit * factor)
                    .min(MAX_CHUNK_SIZE);

                let mut results = self.query_more_results(state, Some(self.current_segment));
                // let mut results = self.search_reader.as_ref().unwrap().search_top_n(
                //     self.search_query_input.as_ref().unwrap(),
                //     self.sort_field.clone(),
                //     self.sort_direction.into(),
                //     self.chunk_size,
                // );

                // fast forward and stop on the ctid we last found
                for (scored, doc_address) in &mut results {
                    if scored.ctid == self.last_ctid {
                        // we've now advanced to the last ctid we found
                        break;
                    }
                }

                // this should be the next valid tuple after that
                next = match results.next() {
                    // ... and there it is!
                    Some(next) => Some(next),

                    // there wasn't one, so we've now read all possible matches
                    None => {
                        return ExecState::Eof;
                    }
                };

                // we now have a new iterator of results to use going forward
                self.search_results = results;

                // but we'll loop back around and evaluate whatever `next` is now pointing to
                continue;
            }
        }
    }
}
