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

use crate::schema::{IndexRecordOption, SearchConfig};
use anyhow::Result;
use core::panic;
use pgrx::PostgresType;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ops::Bound};
use tantivy::{
    collector::DocSetCollector,
    query::{
        AllQuery, BooleanQuery, BoostQuery, ConstScoreQuery, DisjunctionMaxQuery, EmptyQuery,
        ExistsQuery, FastFieldRangeQuery, FuzzyTermQuery, MoreLikeThisQuery, PhrasePrefixQuery,
        PhraseQuery, Query, QueryParser, RangeQuery, RegexQuery, TermQuery, TermSetQuery,
    },
    query_grammar::Occur,
    schema::{Field, FieldType, OwnedValue, DATE_TIME_PRECISION_INDEXED},
    Searcher, Term,
};
use thiserror::Error;

#[derive(Debug, PostgresType, Deserialize, Serialize, Clone, PartialEq, Default)]
pub enum SearchQueryInput {
    All,
    Boolean {
        #[serde(default)]
        #[serde(skip_serializing_if = "Vec::is_empty")]
        must: Vec<SearchQueryInput>,

        #[serde(default)]
        #[serde(skip_serializing_if = "Vec::is_empty")]
        should: Vec<SearchQueryInput>,

        #[serde(default)]
        #[serde(skip_serializing_if = "Vec::is_empty")]
        must_not: Vec<SearchQueryInput>,
    },
    Boost {
        query: Box<SearchQueryInput>,
        boost: f32,
    },
    ConstScore {
        query: Box<SearchQueryInput>,
        score: f32,
    },
    DisjunctionMax {
        disjuncts: Vec<SearchQueryInput>,
        tie_breaker: Option<f32>,
    },
    #[default]
    Empty,
    Exists {
        field: String,
    },
    FastFieldRangeWeight {
        field: String,
        lower_bound: std::ops::Bound<u64>,
        upper_bound: std::ops::Bound<u64>,
    },
    FuzzyTerm {
        field: String,
        value: String,
        distance: Option<u8>,
        transposition_cost_one: Option<bool>,
        prefix: Option<bool>,
        path: Option<String>,
    },
    FuzzyPhrase {
        field: String,
        value: String,
        distance: Option<u8>,
        transposition_cost_one: Option<bool>,
        prefix: Option<bool>,
        match_all_terms: Option<bool>,
        path: Option<String>,
    },
    MoreLikeThis {
        min_doc_frequency: Option<u64>,
        max_doc_frequency: Option<u64>,
        min_term_frequency: Option<usize>,
        max_query_terms: Option<usize>,
        min_word_length: Option<usize>,
        max_word_length: Option<usize>,
        boost_factor: Option<f32>,
        stop_words: Option<Vec<String>>,
        document_fields: Vec<(String, tantivy::schema::OwnedValue)>,
        document_id: Option<tantivy::schema::OwnedValue>,
    },
    Parse {
        query_string: String,
    },
    ParseWithField {
        field: String,
        query_string: String,
    },
    Phrase {
        field: String,
        phrases: Vec<String>,
        slop: Option<u32>,
        path: Option<String>,
    },
    PhrasePrefix {
        field: String,
        phrases: Vec<String>,
        max_expansions: Option<u32>,
        path: Option<String>,
    },
    Range {
        field: String,
        lower_bound: std::ops::Bound<tantivy::schema::OwnedValue>,
        upper_bound: std::ops::Bound<tantivy::schema::OwnedValue>,
        path: Option<String>,
        is_datetime: bool,
    },
    Regex {
        field: String,
        pattern: String,
    },
    Term {
        field: Option<String>,
        value: tantivy::schema::OwnedValue,
        path: Option<String>,
        is_datetime: bool,
    },
    TermSet {
        terms: Vec<(String, tantivy::schema::OwnedValue, Option<String>, bool)>,
    },
}

#[allow(dead_code)]
pub trait AsFieldType<T> {
    fn fields(&self) -> Vec<(FieldType, Field)>;

    fn as_field_type(&self, from: &T) -> Option<(FieldType, Field)>;

    fn is_field_type(&self, from: &T, value: &OwnedValue) -> bool {
        matches!(
            (self.as_field_type(from), value),
            (Some((FieldType::Str(_), _)), OwnedValue::Str(_))
                | (Some((FieldType::U64(_), _)), OwnedValue::U64(_))
                | (Some((FieldType::I64(_), _)), OwnedValue::I64(_))
                | (Some((FieldType::F64(_), _)), OwnedValue::F64(_))
                | (Some((FieldType::Bool(_), _)), OwnedValue::Bool(_))
                | (Some((FieldType::Date(_), _)), OwnedValue::Date(_))
                | (Some((FieldType::Facet(_), _)), OwnedValue::Facet(_))
                | (Some((FieldType::Bytes(_), _)), OwnedValue::Bytes(_))
                | (Some((FieldType::JsonObject(_), _)), OwnedValue::Object(_))
                | (Some((FieldType::IpAddr(_), _)), OwnedValue::IpAddr(_))
        )
    }

    fn as_str(&self, from: &T) -> Option<Field> {
        self.as_field_type(from).and_then(|(ft, field)| match ft {
            FieldType::Str(_) => Some(field),
            _ => None,
        })
    }
    fn as_u64(&self, from: &T) -> Option<Field> {
        self.as_field_type(from).and_then(|(ft, field)| match ft {
            FieldType::U64(_) => Some(field),
            _ => None,
        })
    }
    fn as_i64(&self, from: &T) -> Option<Field> {
        self.as_field_type(from).and_then(|(ft, field)| match ft {
            FieldType::I64(_) => Some(field),
            _ => None,
        })
    }
    fn as_f64(&self, from: &T) -> Option<Field> {
        self.as_field_type(from).and_then(|(ft, field)| match ft {
            FieldType::F64(_) => Some(field),
            _ => None,
        })
    }
    fn as_bool(&self, from: &T) -> Option<Field> {
        self.as_field_type(from).and_then(|(ft, field)| match ft {
            FieldType::Bool(_) => Some(field),
            _ => None,
        })
    }
    fn as_date(&self, from: &T) -> Option<Field> {
        self.as_field_type(from).and_then(|(ft, field)| match ft {
            FieldType::Date(_) => Some(field),
            _ => None,
        })
    }
    fn as_facet(&self, from: &T) -> Option<Field> {
        self.as_field_type(from).and_then(|(ft, field)| match ft {
            FieldType::Facet(_) => Some(field),
            _ => None,
        })
    }
    fn as_bytes(&self, from: &T) -> Option<Field> {
        self.as_field_type(from).and_then(|(ft, field)| match ft {
            FieldType::Bytes(_) => Some(field),
            _ => None,
        })
    }
    fn as_json_object(&self, from: &T) -> Option<Field> {
        self.as_field_type(from).and_then(|(ft, field)| match ft {
            FieldType::JsonObject(_) => Some(field),
            _ => None,
        })
    }
    fn as_ip_addr(&self, from: &T) -> Option<Field> {
        self.as_field_type(from).and_then(|(ft, field)| match ft {
            FieldType::IpAddr(_) => Some(field),
            _ => None,
        })
    }
}

impl SearchQueryInput {
    pub fn into_tantivy_query(
        self,
        field_lookup: &impl AsFieldType<String>,
        parser: &mut QueryParser,
        searcher: &Searcher,
        config: &SearchConfig,
    ) -> Result<Box<dyn Query>, Box<dyn std::error::Error>> {
        match self {
            Self::All => Ok(Box::new(AllQuery)),
            Self::Boolean {
                must,
                should,
                must_not,
            } => {
                let mut subqueries = vec![];
                for input in must {
                    subqueries.push((
                        Occur::Must,
                        input.into_tantivy_query(field_lookup, parser, searcher, config)?,
                    ));
                }
                for input in should {
                    subqueries.push((
                        Occur::Should,
                        input.into_tantivy_query(field_lookup, parser, searcher, config)?,
                    ));
                }
                for input in must_not {
                    subqueries.push((
                        Occur::MustNot,
                        input.into_tantivy_query(field_lookup, parser, searcher, config)?,
                    ));
                }
                Ok(Box::new(BooleanQuery::new(subqueries)))
            }
            Self::Boost { query, boost } => Ok(Box::new(BoostQuery::new(
                query.into_tantivy_query(field_lookup, parser, searcher, config)?,
                boost,
            ))),
            Self::ConstScore { query, score } => Ok(Box::new(ConstScoreQuery::new(
                query.into_tantivy_query(field_lookup, parser, searcher, config)?,
                score,
            ))),
            Self::DisjunctionMax {
                disjuncts,
                tie_breaker,
            } => {
                let disjuncts = disjuncts
                    .into_iter()
                    .map(|query| query.into_tantivy_query(field_lookup, parser, searcher, config))
                    .collect::<Result<_, _>>()?;
                if let Some(tie_breaker) = tie_breaker {
                    Ok(Box::new(DisjunctionMaxQuery::with_tie_breaker(
                        disjuncts,
                        tie_breaker,
                    )))
                } else {
                    Ok(Box::new(DisjunctionMaxQuery::new(disjuncts)))
                }
            }
            Self::Empty => Ok(Box::new(EmptyQuery)),
            Self::Exists { field } => Ok(Box::new(ExistsQuery::new_exists_query(field))),
            Self::FastFieldRangeWeight {
                field,
                lower_bound,
                upper_bound,
            } => {
                let field = field_lookup
                    .as_u64(&field)
                    .or_else(|| field_lookup.as_i64(&field))
                    .ok_or_else(|| QueryError::WrongFieldType(field.clone()))?;

                let new_lower_bound = match lower_bound {
                    Bound::Excluded(v) => Bound::Excluded(Term::from_field_u64(field, v)),
                    Bound::Included(v) => Bound::Included(Term::from_field_u64(field, v)),
                    Bound::Unbounded => Bound::Unbounded,
                };

                let new_upper_bound = match upper_bound {
                    Bound::Excluded(v) => Bound::Excluded(Term::from_field_u64(field, v)),
                    Bound::Included(v) => Bound::Included(Term::from_field_u64(field, v)),
                    Bound::Unbounded => Bound::Unbounded,
                };

                Ok(Box::new(FastFieldRangeQuery::new(
                    new_lower_bound,
                    new_upper_bound,
                )))
            }
            Self::FuzzyTerm {
                field,
                value,
                distance,
                transposition_cost_one,
                prefix,
                path,
            } => {
                let (field_type, field) = field_lookup
                    .as_field_type(&field)
                    .ok_or_else(|| QueryError::NonIndexedField(field))?;
                let term = value_to_term(field, &OwnedValue::Str(value), &field_type, path, false)?;
                let distance = distance.unwrap_or(2);
                let transposition_cost_one = transposition_cost_one.unwrap_or(true);
                if prefix.unwrap_or(false) {
                    Ok(Box::new(FuzzyTermQuery::new_prefix(
                        term,
                        distance,
                        transposition_cost_one,
                    )))
                } else {
                    Ok(Box::new(FuzzyTermQuery::new(
                        term,
                        distance,
                        transposition_cost_one,
                    )))
                }
            }
            Self::FuzzyPhrase {
                field,
                value,
                distance,
                transposition_cost_one,
                prefix,
                match_all_terms,
                path,
            } => {
                let distance = distance.unwrap_or(2);
                let transposition_cost_one = transposition_cost_one.unwrap_or(true);
                let match_all_terms = match_all_terms.unwrap_or(false);
                let prefix = prefix.unwrap_or(false);

                let (field_type, field) = field_lookup
                    .as_field_type(&field)
                    .ok_or_else(|| QueryError::NonIndexedField(field))?;

                let mut analyzer = searcher.index().tokenizer_for_field(field)?;
                let mut stream = analyzer.token_stream(&value);
                let mut terms = Vec::new();

                while stream.advance() {
                    let token = stream.token().text.clone();
                    let term = value_to_term(
                        field,
                        &OwnedValue::Str(token),
                        &field_type,
                        path.clone(),
                        false,
                    )?;
                    let term_query: Box<dyn Query> = if prefix {
                        Box::new(FuzzyTermQuery::new_prefix(
                            term,
                            distance,
                            transposition_cost_one,
                        ))
                    } else {
                        Box::new(FuzzyTermQuery::new(term, distance, transposition_cost_one))
                    };
                    let occur = if match_all_terms {
                        Occur::Must
                    } else {
                        Occur::Should
                    };

                    terms.push((occur, term_query));
                }

                Ok(Box::new(BooleanQuery::new(terms)))
            }
            Self::MoreLikeThis {
                min_doc_frequency,
                max_doc_frequency,
                min_term_frequency,
                max_query_terms,
                min_word_length,
                max_word_length,
                boost_factor,
                stop_words,
                document_fields,
                document_id,
            } => {
                let mut builder = MoreLikeThisQuery::builder();

                if let Some(min_doc_frequency) = min_doc_frequency {
                    builder = builder.with_min_doc_frequency(min_doc_frequency);
                }
                if let Some(max_doc_frequency) = max_doc_frequency {
                    builder = builder.with_max_doc_frequency(max_doc_frequency);
                }
                if let Some(min_term_frequency) = min_term_frequency {
                    builder = builder.with_min_term_frequency(min_term_frequency);
                }
                if let Some(max_query_terms) = max_query_terms {
                    builder = builder.with_max_query_terms(max_query_terms);
                }
                if let Some(min_work_length) = min_word_length {
                    builder = builder.with_min_word_length(min_work_length);
                }
                if let Some(max_work_length) = max_word_length {
                    builder = builder.with_max_word_length(max_work_length);
                }
                if let Some(boost_factor) = boost_factor {
                    builder = builder.with_boost_factor(boost_factor);
                }
                if let Some(stop_words) = stop_words {
                    builder = builder.with_stop_words(stop_words);
                }

                if let Some(key_value) = document_id {
                    let (field_type, field) = field_lookup
                        .as_field_type(&config.key_field)
                        .expect("internal error, key field should be found here");
                    let term = value_to_term(field, &key_value, &field_type, None, false)?;
                    let query: Box<dyn Query> =
                        Box::new(TermQuery::new(term, IndexRecordOption::Basic.into()));
                    let addresses = searcher.search(&query, &DocSetCollector)?;
                    let disjuncts: Vec<Box<dyn Query>> = addresses
                        .into_iter()
                        .map(|address| builder.clone().with_document(address))
                        .map(|query| Box::new(query) as Box<dyn Query>)
                        .collect();
                    return Ok(Box::new(DisjunctionMaxQuery::new(disjuncts)));
                }

                let mut fields_map = HashMap::new();
                for (field_name, value) in document_fields {
                    if !field_lookup.is_field_type(&field_name, &value) {
                        return Err(Box::new(QueryError::WrongFieldType(field_name)));
                    }

                    let (_, field) = field_lookup
                        .as_field_type(&field_name)
                        .ok_or_else(|| QueryError::WrongFieldType(field_name.clone()))?;

                    fields_map.entry(field).or_insert_with(std::vec::Vec::new);

                    if let Some(vec) = fields_map.get_mut(&field) {
                        vec.push(value)
                    }
                }

                Ok(Box::new(
                    builder.with_document_fields(fields_map.into_iter().collect()),
                ))
            }
            Self::PhrasePrefix {
                field,
                phrases,
                max_expansions,
                path,
            } => {
                let (field_type, field) = field_lookup
                    .as_field_type(&field)
                    .ok_or_else(|| QueryError::NonIndexedField(field))?;
                let terms = phrases.clone().into_iter().map(|phrase| {
                    value_to_term(
                        field,
                        &OwnedValue::Str(phrase),
                        &field_type,
                        path.clone(),
                        false,
                    )
                    .unwrap()
                });
                let mut query = PhrasePrefixQuery::new(terms.collect());
                if let Some(max_expansions) = max_expansions {
                    query.set_max_expansions(max_expansions)
                }
                Ok(Box::new(query))
            }
            Self::Parse { query_string } => match config.lenient_parsing {
                Some(true) => {
                    let (parsed_query, _) = parser.parse_query_lenient(&query_string);
                    Ok(Box::new(parsed_query))
                }
                _ => {
                    Ok(Box::new(parser.parse_query(&query_string).map_err(
                        |err| QueryError::ParseError(err, query_string),
                    )?))
                }
            },
            Self::ParseWithField {
                field,
                query_string,
            } => {
                let query_string = format!("{field}:({query_string})");
                Self::Parse { query_string }.into_tantivy_query(
                    field_lookup,
                    parser,
                    searcher,
                    config,
                )
            }
            Self::Phrase {
                field,
                phrases,
                slop,
                path,
            } => {
                let (field_type, field) = field_lookup
                    .as_field_type(&field)
                    .ok_or_else(|| QueryError::NonIndexedField(field))?;
                let terms = phrases.clone().into_iter().map(|phrase| {
                    value_to_term(
                        field,
                        &OwnedValue::Str(phrase),
                        &field_type,
                        path.clone(),
                        false,
                    )
                    .unwrap()
                });
                let mut query = PhraseQuery::new(terms.collect());
                if let Some(slop) = slop {
                    query.set_slop(slop)
                }
                Ok(Box::new(query))
            }
            Self::Range {
                field,
                lower_bound,
                upper_bound,
                path,
                is_datetime,
            } => {
                let field_name = field;
                let (field_type, field) = field_lookup
                    .as_field_type(&field_name)
                    .ok_or_else(|| QueryError::WrongFieldType(field_name.clone()))?;

                let lower_bound = match lower_bound {
                    Bound::Included(value) => Bound::Included(value_to_term(
                        field,
                        &value,
                        &field_type,
                        path.clone(),
                        is_datetime,
                    )?),
                    Bound::Excluded(value) => Bound::Excluded(value_to_term(
                        field,
                        &value,
                        &field_type,
                        path.clone(),
                        is_datetime,
                    )?),
                    Bound::Unbounded => Bound::Unbounded,
                };

                let upper_bound = match upper_bound {
                    Bound::Included(value) => Bound::Included(value_to_term(
                        field,
                        &value,
                        &field_type,
                        path.clone(),
                        is_datetime,
                    )?),
                    Bound::Excluded(value) => Bound::Excluded(value_to_term(
                        field,
                        &value,
                        &field_type,
                        path.clone(),
                        is_datetime,
                    )?),
                    Bound::Unbounded => Bound::Unbounded,
                };

                Ok(Box::new(RangeQuery::new(lower_bound, upper_bound)))
            }
            Self::Regex { field, pattern } => Ok(Box::new(
                RegexQuery::from_pattern(
                    &pattern,
                    field_lookup
                        .as_str(&field)
                        .ok_or_else(|| QueryError::WrongFieldType(field.clone()))?,
                )
                .map_err(|err| QueryError::RegexError(err, pattern.clone()))?,
            )),
            Self::Term {
                field,
                value,
                path,
                is_datetime,
            } => {
                let record_option = IndexRecordOption::WithFreqsAndPositions;
                if let Some(field) = field {
                    let (field_type, field) = field_lookup
                        .as_field_type(&field)
                        .ok_or_else(|| QueryError::NonIndexedField(field))?;
                    let term = value_to_term(field, &value, &field_type, path, is_datetime)?;

                    Ok(Box::new(TermQuery::new(term, record_option.into())))
                } else {
                    // If no field is passed, then search all fields.
                    let all_fields = field_lookup.fields();
                    let mut terms = vec![];
                    for (field_type, field) in all_fields {
                        if let Ok(term) =
                            value_to_term(field, &value, &field_type, None, is_datetime)
                        {
                            terms.push(term);
                        }
                    }

                    Ok(Box::new(TermSetQuery::new(terms)))
                }
            }
            Self::TermSet { terms: fields } => {
                let mut terms = vec![];
                for (field_name, field_value, path, is_datetime) in fields {
                    let (field_type, field) = field_lookup
                        .as_field_type(&field_name)
                        .ok_or_else(|| QueryError::NonIndexedField(field_name))?;
                    terms.push(value_to_term(
                        field,
                        &field_value,
                        &field_type,
                        path,
                        is_datetime,
                    )?);
                }

                Ok(Box::new(TermSetQuery::new(terms)))
            }
        }
    }
}

fn value_to_json_term(
    field: Field,
    value: &OwnedValue,
    path: Option<String>,
    expand_dots: bool,
    is_datetime: bool,
) -> Result<Term, Box<dyn std::error::Error>> {
    let mut term = Term::from_field_json_path(field, &path.unwrap_or_default(), expand_dots);
    match value {
        OwnedValue::Str(text) => {
            if is_datetime {
                let TantivyDateTime(date) = TantivyDateTime::try_from(text.as_str())?;
                // https://github.com/quickwit-oss/tantivy/pull/2456
                // It's a footgun that date needs to truncated when creating the Term
                term.append_type_and_fast_value(date.truncate(DATE_TIME_PRECISION_INDEXED));
            } else {
                term.append_type_and_str(text);
            }
        }
        OwnedValue::U64(value) => {
            if let Ok(i64_val) = (*value).try_into() {
                term.append_type_and_fast_value::<i64>(i64_val);
            } else {
                term.append_type_and_fast_value(*value);
            }
        }
        OwnedValue::I64(value) => {
            term.append_type_and_fast_value(*value);
        }
        OwnedValue::F64(value) => {
            term.append_type_and_fast_value(*value);
        }
        OwnedValue::Bool(value) => {
            term.append_type_and_fast_value(*value);
        }
        OwnedValue::Date(value) => {
            term.append_type_and_fast_value(*value);
        }
        unsupported => panic!(
            "Tantivy OwnedValue type {:?} not supported for JSON term",
            unsupported
        ),
    };

    Ok(term)
}

fn value_to_term(
    field: Field,
    value: &OwnedValue,
    field_type: &FieldType,
    path: Option<String>,
    is_datetime: bool,
) -> Result<Term, Box<dyn std::error::Error>> {
    let json_options = match field_type {
        FieldType::JsonObject(ref options) => Some(options),
        _ => None,
    };

    if let Some(json_options) = json_options {
        return value_to_json_term(
            field,
            value,
            path,
            json_options.is_expand_dots_enabled(),
            is_datetime,
        );
    }

    if is_datetime {
        if let OwnedValue::Str(text) = value {
            let TantivyDateTime(date) = TantivyDateTime::try_from(text.as_str())?;
            // https://github.com/quickwit-oss/tantivy/pull/2456
            // It's a footgun that date needs to truncated when creating the Term
            return Ok(Term::from_field_date(
                field,
                date.truncate(DATE_TIME_PRECISION_INDEXED),
            ));
        }
    }

    Ok(match value {
        OwnedValue::Str(text) => Term::from_field_text(field, text),
        OwnedValue::PreTokStr(_) => panic!("pre-tokenized text cannot be converted to term"),
        OwnedValue::U64(u64) => {
            // Positive numbers seem to be automatically turned into u64s even if they are i64s,
            // so we should use the field type to assign the term type
            match field_type {
                FieldType::I64(_) => Term::from_field_i64(field, *u64 as i64),
                FieldType::U64(_) => Term::from_field_u64(field, *u64),
                _ => panic!("invalid field type for u64 value"),
            }
        }
        OwnedValue::I64(i64) => Term::from_field_i64(field, *i64),
        OwnedValue::F64(f64) => Term::from_field_f64(field, *f64),
        OwnedValue::Bool(bool) => Term::from_field_bool(field, *bool),
        OwnedValue::Date(date) => {
            Term::from_field_date(field, date.truncate(DATE_TIME_PRECISION_INDEXED))
        }
        OwnedValue::Facet(facet) => Term::from_facet(field, facet),
        OwnedValue::Bytes(bytes) => Term::from_field_bytes(field, bytes),
        OwnedValue::Object(_) => panic!("json cannot be converted to term"),
        OwnedValue::IpAddr(ip) => Term::from_field_ip_addr(field, *ip),
        _ => panic!("Tantivy OwnedValue type not supported"),
    })
}

struct TantivyDateTime(pub tantivy::DateTime);
impl TryFrom<&str> for TantivyDateTime {
    type Error = QueryError;

    fn try_from(text: &str) -> Result<Self, Self::Error> {
        let datetime = match chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%SZ") {
            Ok(dt) => dt,
            Err(_) => chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.fZ")
                .map_err(|_| QueryError::FieldTypeMismatch)?,
        };
        Ok(TantivyDateTime(tantivy::DateTime::from_timestamp_micros(
            datetime.and_utc().timestamp_micros(),
        )))
    }
}

#[allow(dead_code)]
#[derive(Debug, Error)]
enum QueryError {
    #[error("wrong field type for field: {0}")]
    WrongFieldType(String),
    #[error("invalid field map json: {0}")]
    FieldMapJsonValue(#[source] serde_json::Error),
    #[error("field map json must be an object")]
    FieldMapJsonObject,
    #[error("field '{0}' is not part of the pg_search index")]
    NonIndexedField(String),
    #[error("wrong type given for field")]
    FieldTypeMismatch,
    #[error("could not build regex with pattern '{1}': {0}")]
    RegexError(#[source] tantivy::TantivyError, String),
    #[error(
        r#"could not parse query string '{1}'.
           make sure to use column:term pairs, and to capitalize AND/OR."#
    )]
    ParseError(#[source] tantivy::query::QueryParserError, String),
}
