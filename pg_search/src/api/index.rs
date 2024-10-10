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

use pgrx::datum::RangeBound;
use pgrx::{iter::TableIterator, *};

use crate::index::SearchIndex;
use crate::index::WriterDirectory;
use crate::postgres::types::TantivyValue;
use crate::postgres::utils::{index_oid_from_index_name, relfilenode_from_index_oid};
use crate::query::SearchQueryInput;
use crate::schema::IndexRecordOption;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::CStr;
use std::fmt::{Display, Formatter};
use std::ops::Bound;
use tantivy::json_utils::split_json_path;
use tantivy::schema::{FieldType, OwnedValue, Value};

#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn schema_bm25(
    index_name: &str,
) -> TableIterator<(
    name!(name, String),
    name!(field_type, String),
    name!(stored, bool),
    name!(indexed, bool),
    name!(fast, bool),
    name!(fieldnorms, bool),
    name!(expand_dots, Option<bool>),
    name!(tokenizer, Option<String>),
    name!(record, Option<String>),
    name!(normalizer, Option<String>),
)> {
    let bm25_index_name = format!("{}_bm25_index", index_name);

    let database_oid = crate::MyDatabaseId();
    let index_oid = index_oid_from_index_name(&bm25_index_name);
    let relfilenode = relfilenode_from_index_oid(index_oid.as_u32());

    let directory =
        WriterDirectory::from_oids(database_oid, index_oid.as_u32(), relfilenode.as_u32());
    let search_index = SearchIndex::from_disk(&directory)
        .unwrap_or_else(|err| panic!("error loading index from directory: {err}"));

    let schema = search_index.schema.schema.clone();
    let mut field_entries: Vec<_> = schema.fields().collect();

    // To ensure consistent ordering of outputs, we'll sort the results by field name.
    field_entries.sort_by_key(|(field, _)| schema.get_field_name(*field).to_string());

    let mut field_rows = Vec::new();

    for field in field_entries {
        let (field, field_entry) = field;
        let name = schema.get_field_name(field).to_string();

        let (field_type, tokenizer, record, normalizer, expand_dots) =
            match field_entry.field_type() {
                FieldType::I64(_) => ("I64".to_string(), None, None, None, None),
                FieldType::U64(_) => ("U64".to_string(), None, None, None, None),
                FieldType::F64(_) => ("F64".to_string(), None, None, None, None),
                FieldType::Bool(_) => ("Bool".to_string(), None, None, None, None),
                FieldType::Str(text_options) => {
                    let indexing_options = text_options.get_indexing_options();
                    let tokenizer = indexing_options.map(|opt| opt.tokenizer().to_string());
                    let record = indexing_options
                        .map(|opt| IndexRecordOption::from(opt.index_option()).to_string());
                    let normalizer = text_options
                        .get_fast_field_tokenizer_name()
                        .map(|s| s.to_string());
                    ("Str".to_string(), tokenizer, record, normalizer, None)
                }
                FieldType::JsonObject(json_options) => {
                    let indexing_options = json_options.get_text_indexing_options();
                    let tokenizer = indexing_options.map(|opt| opt.tokenizer().to_string());
                    let record = indexing_options
                        .map(|opt| IndexRecordOption::from(opt.index_option()).to_string());
                    let normalizer = json_options
                        .get_fast_field_tokenizer_name()
                        .map(|s| s.to_string());
                    let expand_dots = Some(json_options.is_expand_dots_enabled());
                    (
                        "JsonObject".to_string(),
                        tokenizer,
                        record,
                        normalizer,
                        expand_dots,
                    )
                }
                FieldType::Date(_) => ("Date".to_string(), None, None, None, None),
                _ => ("Other".to_string(), None, None, None, None),
            };

        let row = (
            name,
            field_type,
            field_entry.is_stored(),
            field_entry.is_indexed(),
            field_entry.is_fast(),
            field_entry.has_fieldnorms(),
            expand_dots,
            tokenizer,
            record,
            normalizer,
        );

        field_rows.push(row);
    }

    TableIterator::new(field_rows)
}

#[pg_extern(immutable, parallel_safe)]
pub fn all() -> SearchQueryInput {
    SearchQueryInput::All
}

#[pg_extern(name = "boolean", immutable, parallel_safe)]
pub fn boolean_arrays(
    must: default!(Vec<SearchQueryInput>, "ARRAY[]::searchqueryinput[]"),
    should: default!(Vec<SearchQueryInput>, "ARRAY[]::searchqueryinput[]"),
    must_not: default!(Vec<SearchQueryInput>, "ARRAY[]::searchqueryinput[]"),
) -> SearchQueryInput {
    SearchQueryInput::Boolean {
        must,
        should,
        must_not,
    }
}

#[pg_extern(name = "boolean", immutable, parallel_safe)]
pub fn boolean_singles(
    must: default!(Option<SearchQueryInput>, "NULL"),
    should: default!(Option<SearchQueryInput>, "NULL"),
    must_not: default!(Option<SearchQueryInput>, "NULL"),
) -> SearchQueryInput {
    boolean_arrays(
        must.map_or(vec![], |v| vec![v]),
        should.map_or(vec![], |v| vec![v]),
        must_not.map_or(vec![], |v| vec![v]),
    )
}

#[pg_extern(immutable, parallel_safe)]
pub fn boost(boost: f32, query: SearchQueryInput) -> SearchQueryInput {
    SearchQueryInput::Boost {
        query: Box::new(query),
        boost,
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn const_score(score: f32, query: SearchQueryInput) -> SearchQueryInput {
    SearchQueryInput::ConstScore {
        query: Box::new(query),
        score,
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn disjunction_max(
    disjuncts: Array<SearchQueryInput>,
    tie_breaker: default!(Option<f32>, "NULL"),
) -> SearchQueryInput {
    SearchQueryInput::DisjunctionMax {
        disjuncts: disjuncts.iter_deny_null().collect(),
        tie_breaker,
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn empty() -> SearchQueryInput {
    SearchQueryInput::Empty
}

#[pg_extern(immutable, parallel_safe)]
pub fn exists(field: FieldName) -> SearchQueryInput {
    SearchQueryInput::Exists {
        field: field.into_inner(),
    }
}

// Not clear on whether this query makes sense to support, as only our "key_field" is a fast
// field... and the user can just use SQL to select a range. We'll keep the implementation here
// for now, but we should remove when we decide definitively that we don't need this.
#[allow(unused)]
pub fn fast_field_range_weight(
    field: FieldName,
    range: default!(Option<pgrx::Range<i32>>, "NULL"),
) -> SearchQueryInput {
    match range.expect("`range` argument is required").into_inner() {
        None => SearchQueryInput::FastFieldRangeWeight {
            field: field.into_inner(),
            lower_bound: Bound::Included(0),
            upper_bound: Bound::Excluded(0),
        },
        Some((lower, upper)) => SearchQueryInput::FastFieldRangeWeight {
            field: field.into_inner(),
            lower_bound: match lower {
                RangeBound::Infinite => Bound::Unbounded,
                RangeBound::Inclusive(n) => Bound::Included(n as u64),
                RangeBound::Exclusive(n) => Bound::Excluded(n as u64),
            },
            upper_bound: match upper {
                RangeBound::Infinite => Bound::Unbounded,
                RangeBound::Inclusive(n) => Bound::Included(n as u64),
                RangeBound::Exclusive(n) => Bound::Excluded(n as u64),
            },
        },
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn fuzzy_term(
    field: FieldName,
    value: default!(Option<String>, "NULL"),
    distance: default!(Option<i32>, "NULL"),
    transposition_cost_one: default!(Option<bool>, "NULL"),
    prefix: default!(Option<bool>, "NULL"),
) -> SearchQueryInput {
    let (field, path) = split_field_and_path(&field);
    SearchQueryInput::FuzzyTerm {
        field,
        value: value.expect("`value` argument is required"),
        distance: distance.map(|n| n as u8),
        transposition_cost_one,
        prefix,
        path,
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn fuzzy_phrase(
    field: FieldName,
    value: default!(Option<String>, "NULL"),
    distance: default!(Option<i32>, "NULL"),
    transposition_cost_one: default!(Option<bool>, "NULL"),
    prefix: default!(Option<bool>, "NULL"),
    match_all_terms: default!(Option<bool>, "NULL"),
) -> SearchQueryInput {
    let (field, path) = split_field_and_path(&field);
    SearchQueryInput::FuzzyPhrase {
        field,
        value: value.expect("`value` argument is required"),
        distance: distance.map(|n| n as u8),
        transposition_cost_one,
        prefix,
        match_all_terms,
        path,
    }
}

#[pg_extern(name = "more_like_this", immutable, parallel_safe)]
pub fn more_like_this_empty() -> SearchQueryInput {
    panic!("more_like_this must be called with either with_document_id or with_document_fields");
}

#[allow(clippy::too_many_arguments)]
#[pg_extern(name = "more_like_this", immutable, parallel_safe)]
pub fn more_like_this_fields(
    with_document_fields: String,
    with_min_doc_frequency: default!(Option<i32>, "NULL"),
    with_max_doc_frequency: default!(Option<i32>, "NULL"),
    with_min_term_frequency: default!(Option<i32>, "NULL"),
    with_max_query_terms: default!(Option<i32>, "NULL"),
    with_min_word_length: default!(Option<i32>, "NULL"),
    with_max_word_length: default!(Option<i32>, "NULL"),
    with_boost_factor: default!(Option<f32>, "NULL"),
    with_stop_words: default!(Option<Vec<String>>, "NULL"),
) -> SearchQueryInput {
    let document_fields: HashMap<String, tantivy::schema::OwnedValue> =
        json5::from_str(&with_document_fields).expect("could not parse with_document_fields");

    SearchQueryInput::MoreLikeThis {
        min_doc_frequency: with_min_doc_frequency.map(|n| n as u64),
        max_doc_frequency: with_max_doc_frequency.map(|n| n as u64),
        min_term_frequency: with_min_term_frequency.map(|n| n as usize),
        max_query_terms: with_max_query_terms.map(|n| n as usize),
        min_word_length: with_min_word_length.map(|n| n as usize),
        max_word_length: with_max_word_length.map(|n| n as usize),
        boost_factor: with_boost_factor,
        stop_words: with_stop_words,
        document_fields: document_fields.into_iter().collect(),
        document_id: None,
    }
}

#[allow(clippy::too_many_arguments)]
#[pg_extern(name = "more_like_this", immutable, parallel_safe)]
pub fn more_like_this_id(
    with_document_id: AnyElement,
    with_min_doc_frequency: default!(Option<i32>, "NULL"),
    with_max_doc_frequency: default!(Option<i32>, "NULL"),
    with_min_term_frequency: default!(Option<i32>, "NULL"),
    with_max_query_terms: default!(Option<i32>, "NULL"),
    with_min_word_length: default!(Option<i32>, "NULL"),
    with_max_word_length: default!(Option<i32>, "NULL"),
    with_boost_factor: default!(Option<f32>, "NULL"),
    with_stop_words: default!(Option<Vec<String>>, "NULL"),
) -> SearchQueryInput {
    SearchQueryInput::MoreLikeThis {
        min_doc_frequency: with_min_doc_frequency.map(|n| n as u64),
        max_doc_frequency: with_max_doc_frequency.map(|n| n as u64),
        min_term_frequency: with_min_term_frequency.map(|n| n as usize),
        max_query_terms: with_max_query_terms.map(|n| n as usize),
        min_word_length: with_min_word_length.map(|n| n as usize),
        max_word_length: with_max_word_length.map(|n| n as usize),
        boost_factor: with_boost_factor,
        stop_words: with_stop_words,
        document_fields: vec![],
        document_id: unsafe {
            Some(
                TantivyValue::try_from_datum(
                    with_document_id.datum(),
                    PgOid::from_untagged(with_document_id.oid()),
                )
                .unwrap_or_else(|err| panic!("could not read more_like_this document_id: {err}"))
                .0,
            )
        },
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn parse(query_string: String) -> SearchQueryInput {
    SearchQueryInput::Parse { query_string }
}

#[pg_extern(immutable, parallel_safe)]
pub fn parse_with_field(field: FieldName, query_string: String) -> SearchQueryInput {
    SearchQueryInput::ParseWithField {
        field: field.into_inner(),
        query_string,
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn phrase(
    field: FieldName,
    phrases: Vec<String>,
    slop: default!(Option<i32>, "NULL"),
) -> SearchQueryInput {
    let (field, path) = split_field_and_path(&field);
    SearchQueryInput::Phrase {
        field,
        phrases,
        slop: slop.map(|n| n as u32),
        path,
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn phrase_prefix(
    field: FieldName,
    phrases: Vec<String>,
    max_expansion: default!(Option<i32>, "NULL"),
) -> SearchQueryInput {
    let (field, path) = split_field_and_path(&field);
    SearchQueryInput::PhrasePrefix {
        field,
        phrases,
        max_expansions: max_expansion.map(|n| n as u32),
        path,
    }
}

#[pg_extern(name = "range", immutable, parallel_safe)]
pub fn range_i32(field: FieldName, range: Range<i32>) -> SearchQueryInput {
    let (field, path) = split_field_and_path(&field);
    match range.into_inner() {
        None => SearchQueryInput::Range {
            field,
            lower_bound: Bound::Included(OwnedValue::I64(0)),
            upper_bound: Bound::Excluded(OwnedValue::I64(0)),
            path,
            is_datetime: false,
        },
        Some((lower, upper)) => SearchQueryInput::Range {
            field,
            lower_bound: match lower {
                RangeBound::Infinite => Bound::Unbounded,
                RangeBound::Inclusive(n) => Bound::Included(OwnedValue::I64(n as i64)),
                RangeBound::Exclusive(n) => Bound::Excluded(OwnedValue::I64(n as i64)),
            },
            upper_bound: match upper {
                RangeBound::Infinite => Bound::Unbounded,
                RangeBound::Inclusive(n) => Bound::Included(OwnedValue::I64(n as i64)),
                RangeBound::Exclusive(n) => Bound::Excluded(OwnedValue::I64(n as i64)),
            },
            path,
            is_datetime: false,
        },
    }
}

#[pg_extern(name = "range", immutable, parallel_safe)]
pub fn range_i64(field: FieldName, range: Range<i64>) -> SearchQueryInput {
    let (field, path) = split_field_and_path(&field);
    match range.into_inner() {
        None => SearchQueryInput::Range {
            field,
            lower_bound: Bound::Included(OwnedValue::I64(0)),
            upper_bound: Bound::Excluded(OwnedValue::I64(0)),
            path,
            is_datetime: false,
        },
        Some((lower, upper)) => SearchQueryInput::Range {
            field,
            lower_bound: match lower {
                RangeBound::Infinite => Bound::Unbounded,
                RangeBound::Inclusive(n) => Bound::Included(OwnedValue::I64(n)),
                RangeBound::Exclusive(n) => Bound::Excluded(OwnedValue::I64(n)),
            },
            upper_bound: match upper {
                RangeBound::Infinite => Bound::Unbounded,
                RangeBound::Inclusive(n) => Bound::Included(OwnedValue::I64(n)),
                RangeBound::Exclusive(n) => Bound::Excluded(OwnedValue::I64(n)),
            },
            path,
            is_datetime: false,
        },
    }
}

#[pg_extern(name = "range", immutable, parallel_safe)]
pub fn range_numeric(field: FieldName, range: Range<AnyNumeric>) -> SearchQueryInput {
    let (field, path) = split_field_and_path(&field);
    match range.into_inner() {
        None => SearchQueryInput::Range {
            field,
            lower_bound: Bound::Included(OwnedValue::F64(0.0)),
            upper_bound: Bound::Excluded(OwnedValue::F64(0.0)),
            path,
            is_datetime: false,
        },
        Some((lower, upper)) => SearchQueryInput::Range {
            field,
            lower_bound: match lower {
                RangeBound::Infinite => Bound::Unbounded,
                RangeBound::Inclusive(n) => Bound::Included(OwnedValue::F64(
                    n.try_into().expect("numeric should be a valid f64"),
                )),
                RangeBound::Exclusive(n) => Bound::Excluded(OwnedValue::F64(
                    n.try_into().expect("numeric should be a valid f64"),
                )),
            },
            upper_bound: match upper {
                RangeBound::Infinite => Bound::Unbounded,
                RangeBound::Inclusive(n) => Bound::Included(OwnedValue::F64(
                    n.try_into().expect("numeric should be a valid f64"),
                )),
                RangeBound::Exclusive(n) => Bound::Excluded(OwnedValue::F64(
                    n.try_into().expect("numeric should be a valid f64"),
                )),
            },
            path,
            is_datetime: false,
        },
    }
}

macro_rules! datetime_range_fn {
    ($func_name:ident, $value_type:ty) => {
        #[pg_extern(name = "range", immutable, parallel_safe)]
        pub fn $func_name(field: FieldName, range: Range<$value_type>) -> SearchQueryInput {
            let (field, path) = split_field_and_path(&field);
            match range.into_inner() {
                None => SearchQueryInput::Range {
                    field,
                    lower_bound: Bound::Included(tantivy::schema::OwnedValue::Date(
                        tantivy::DateTime::from_timestamp_micros(0),
                    )),
                    upper_bound: Bound::Excluded(tantivy::schema::OwnedValue::Date(
                        tantivy::DateTime::from_timestamp_micros(0),
                    )),
                    path,
                    is_datetime: true,
                },
                Some((lower, upper)) => SearchQueryInput::Range {
                    field,
                    lower_bound: match lower {
                        RangeBound::Infinite => Bound::Unbounded,
                        RangeBound::Inclusive(n) => Bound::Included(
                            (&TantivyValue::try_from(n)
                                .expect("n should be a valid TantivyValue representation")
                                .tantivy_schema_value())
                                .as_datetime()
                                .expect("OwnedValue should be a valid datetime value")
                                .into(),
                        ),
                        RangeBound::Exclusive(n) => Bound::Excluded(
                            (&TantivyValue::try_from(n)
                                .expect("n should be a valid TantivyValue representation")
                                .tantivy_schema_value())
                                .as_datetime()
                                .expect("OwnedValue should be a valid datetime value")
                                .into(),
                        ),
                    },
                    upper_bound: match upper {
                        RangeBound::Infinite => Bound::Unbounded,
                        RangeBound::Inclusive(n) => Bound::Included(
                            (&TantivyValue::try_from(n)
                                .expect("n should be a valid TantivyValue representation")
                                .tantivy_schema_value())
                                .as_datetime()
                                .expect("OwnedValue should be a valid datetime value")
                                .into(),
                        ),
                        RangeBound::Exclusive(n) => Bound::Excluded(
                            (&TantivyValue::try_from(n)
                                .expect("n should be a valid TantivyValue representation")
                                .tantivy_schema_value())
                                .as_datetime()
                                .expect("OwnedValue should be a valid datetime value")
                                .into(),
                        ),
                    },
                    path,
                    is_datetime: true,
                },
            }
        }
    };
}

datetime_range_fn!(range_date, pgrx::datum::Date);
datetime_range_fn!(range_timestamp, pgrx::datum::Timestamp);
datetime_range_fn!(range_timestamptz, pgrx::datum::TimestampWithTimeZone);

#[pg_extern(immutable, parallel_safe)]
pub fn regex(field: FieldName, pattern: String) -> SearchQueryInput {
    SearchQueryInput::Regex {
        field: field.into_inner(),
        pattern,
    }
}

macro_rules! term_fn {
    ($func_name:ident, $value_type:ty) => {
        #[pg_extern(name = "term", immutable, parallel_safe)]
        pub fn $func_name(
            field: default!(Option<FieldName>, "NULL"),
            value: default!(Option<$value_type>, "NULL"),
        ) -> SearchQueryInput {
            if let Some(value) = value {
                let (field, path) = if let Some(field) = field {
                    let (field, path) = split_field_and_path(&field);
                    (Some(field), path)
                } else {
                    (None, None)
                };

                let tantivy_value = TantivyValue::try_from(value)
                    .expect("value should be a valid TantivyValue representation")
                    .tantivy_schema_value();
                let is_datetime = match tantivy_value {
                    OwnedValue::Date(_) => true,
                    _ => false,
                };

                SearchQueryInput::Term {
                    field,
                    value: tantivy_value,
                    path,
                    is_datetime,
                }
            } else {
                panic!("no value provided to term query")
            }
        }
    };
}

macro_rules! term_fn_unsupported {
    ($func_name:ident, $value_type:ty, $term_type:literal) => {
        #[pg_extern(name = "term", immutable, parallel_safe)]
        #[allow(unused)]
        pub fn $func_name(
            field: FieldName,
            value: default!(Option<$value_type>, "NULL"),
        ) -> SearchQueryInput {
            unimplemented!("{} in term query not implemented", $term_type)
        }
    };
}

// Generate functions for each type
// NOTE: We cannot use AnyElement for `term` because it sullies the user experience.
//       For example, searching for a string value is an ambiguous type, so the user
//       would have to search for 'string'::text or 'string'::varchar in the `value`
//       argument.
term_fn!(term_bytes, Vec<u8>);
term_fn!(term_str, String);
term_fn!(term_i8, i8);
term_fn!(term_i16, i16);
term_fn!(term_i32, i32);
term_fn!(term_i64, i64);
term_fn!(term_f32, f32);
term_fn!(term_f64, f64);
term_fn!(term_bool, bool);
term_fn!(date, pgrx::datum::Date);
term_fn!(time, pgrx::datum::Time);
term_fn!(timestamp, pgrx::datum::Timestamp);
term_fn!(time_with_time_zone, pgrx::datum::TimeWithTimeZone);
term_fn!(timestamp_with_time_zome, pgrx::datum::TimestampWithTimeZone);
term_fn!(numeric, pgrx::AnyNumeric);
term_fn!(uuid, pgrx::Uuid);
term_fn_unsupported!(json, pgrx::Json, "json");
term_fn_unsupported!(jsonb, pgrx::JsonB, "jsonb");
term_fn_unsupported!(anyarray, pgrx::AnyArray, "array");
term_fn_unsupported!(pg_box, pgrx::pg_sys::BOX, "box");
term_fn_unsupported!(point, pgrx::pg_sys::Point, "point");
term_fn_unsupported!(tid, pgrx::pg_sys::ItemPointerData, "tid");
term_fn_unsupported!(inet, pgrx::Inet, "inet");
term_fn_unsupported!(int4range, pgrx::Range<i32>, "int4 range");
term_fn_unsupported!(int8range, pgrx::Range<i64>, "int8 range");
term_fn_unsupported!(numrange, pgrx::Range<pgrx::AnyNumeric>, "numeric range");
term_fn_unsupported!(daterange, pgrx::Range<pgrx::datum::Date>, "date range");
term_fn_unsupported!(
    tsrange,
    pgrx::Range<pgrx::datum::Timestamp>,
    "timestamp range"
);
term_fn_unsupported!(
    tstzrange,
    pgrx::Range<pgrx::datum::TimestampWithTimeZone>,
    "timestamp ranges with time zone"
);

#[pg_extern(immutable, parallel_safe)]
pub fn term_set(
    terms: default!(Vec<SearchQueryInput>, "ARRAY[]::searchqueryinput[]"),
) -> SearchQueryInput {
    let terms: Vec<_> = terms
        .into_iter()
        .filter_map(|input| match input {
            SearchQueryInput::Term {
                field,
                value,
                path,
                is_datetime,
                ..
            } => field.map(|field| (field, value, path, is_datetime)),
            _ => panic!("only term queries can be passed to term_set"),
        })
        .collect();

    SearchQueryInput::TermSet { terms }
}

/// A type used whenever our builder functions require a fieldname.
#[derive(
    Debug, Clone, Ord, Eq, PartialOrd, PartialEq, Hash, Serialize, Deserialize, PostgresType,
)]
#[inoutfuncs]
pub struct FieldName(String);

impl Display for FieldName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for FieldName {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<String> for FieldName {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl InOutFuncs for FieldName {
    fn input(input: &CStr) -> Self
    where
        Self: Sized,
    {
        FieldName(input.to_str().unwrap().to_owned())
    }

    fn output(&self, buffer: &mut StringInfo) {
        buffer.push_str(&self.0);
    }
}

impl FieldName {
    #[inline(always)]
    pub fn into_inner(self) -> String {
        self.0
    }
}

#[pg_cast(implicit)]
fn text_to_fieldname(field: String) -> FieldName {
    FieldName(field)
}

#[allow(unused)]
pub fn fieldname_typoid() -> pg_sys::Oid {
    unsafe {
        let oid = direct_function_call::<pg_sys::Oid>(
            pg_sys::regtypein,
            &[c"paradedb.FieldName".into_datum()],
        )
        .expect("type `paradedb.FieldName` should exist");
        if oid == pg_sys::Oid::INVALID {
            panic!("type `paradedb.FieldName` should exist");
        }
        oid
    }
}

#[inline]
pub fn split_field_and_path(field: &FieldName) -> (String, Option<String>) {
    let json_path = split_json_path(&field.clone().into_inner());
    if json_path.len() == 1 {
        (field.clone().into_inner(), None)
    } else {
        (json_path[0].clone(), Some(json_path[1..].join(".")))
    }
}
