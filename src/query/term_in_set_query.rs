use crate::common::BitSet;
use crate::query::explanation::does_not_match;
use crate::query::{BitSetDocSet, ConstScorer, Explanation, Query, Scorer, Weight};
use crate::schema::{Field, IndexRecordOption, Type, FieldType, IntOptions};
use crate::termdict::{TermDictionaryBuilder, TermDictionary};
use crate::{Result, SegmentReader, SkipResult};
use crate::{Searcher, TantivyError, Term};
use std::collections::HashSet;
use std::sync::Arc;
use crate::directory::ReadOnlySource;
use std::cmp::Ordering;

#[derive(Clone, Debug)]
pub struct TermInSetQuery {
    field: Field,
    value_type: Type,
    sorted_values: Arc<Vec<Vec<u8>>>,
}

impl TermInSetQuery {
    pub fn new_u64(field: Field, values: &[u64]) -> Self {
        let mut term = Term::from_field_u64(field, 42);

        let mut sorted_values = values.clone().into_iter().map(|v| {
            term.set_u64(*v);
            term.value_bytes().to_vec()
        }).collect::<Vec<_>>();
        sorted_values.sort_by(|a, b| a.cmp(b));

        TermInSetQuery {
            field,
            value_type: Type::U64,
            sorted_values: Arc::new(sorted_values),
        }
    }
}

impl Query for TermInSetQuery {
    fn weight(&self, searcher: &Searcher, _scoring_enabled: bool) -> Result<Box<dyn Weight>> {
        let schema = searcher.schema();

        let value_type = schema.get_field_entry(self.field).field_type().value_type();
        if value_type != self.value_type {
            let err_msg = format!(
                "Create a range query of the type {:?}, when the field given was of type {:?}",
                self.value_type, value_type
            );
            return Err(TantivyError::SchemaError(err_msg));
        }

        Ok(Box::new(SetMembershipWeight {
            field: self.field,
            sorted_values: self.sorted_values.clone(),
        }))
    }
}

struct SetMembershipWeight {
    field: Field,
    sorted_values: Arc<Vec<Vec<u8>>>,
}

impl Weight for SetMembershipWeight {
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<dyn Scorer>> {
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);

        let inverted_index = reader.inverted_index(self.field);
        let term_dict = inverted_index.terms();

        let mut idx_values = term_dict.stream();

        let mut sorted_values_iter = self.sorted_values.iter();

        let mut has_set_value = sorted_values_iter.next();
        let mut has_idx_value = idx_values.advance();
        'outer: while has_set_value.is_some() && has_idx_value {
            let set_value = has_set_value.unwrap();
                let idx_value = idx_values.key();

                match set_value.as_slice().cmp(idx_value) {
                    Ordering::Equal => {
                        let term_info = idx_values.value();
                        let mut block_segment_postings = inverted_index
                            .read_block_postings_from_terminfo(&term_info, IndexRecordOption::Basic);
                        while block_segment_postings.advance() {
                            for &doc in block_segment_postings.docs() {
                                doc_bitset.insert(doc);
                            }
                        }
                        has_set_value = sorted_values_iter.next();
                        has_idx_value = idx_values.advance();
                        continue 'outer;
                    },
                    Ordering::Less => {
                        has_set_value = sorted_values_iter.next();
                        continue 'outer;
                    }
                    Ordering::Greater => {
                        has_idx_value = idx_values.advance();
                        continue 'outer;
                    }
                }
        }

        let doc_bitset = BitSetDocSet::from(doc_bitset);
        Ok(Box::new(ConstScorer::new(doc_bitset)))
    }

    fn explain(&self, reader: &SegmentReader, doc: u32) -> Result<Explanation> {
        let mut scorer = self.scorer(reader)?;
        if scorer.skip_next(doc) != SkipResult::Reached {
            return Err(does_not_match(doc));
        }
        Ok(Explanation::new("TermInSetQuery", 1.0f32))
    }
}

#[cfg(test)]
mod tests {
    use super::TermInSetQuery;
    use crate::collector::Count;
    use crate::schema::{Schema, INDEXED};
    use crate::Index;

    #[test]
    fn test_term_in_set_query_basic() {
        let mut schema_builder = Schema::builder();
        let numeric_field = schema_builder.add_u64_field("id", INDEXED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(1, 6_000_000).unwrap();
            for x in 0u64..10_000 {
                index_writer.add_document(doc!(numeric_field => x));
            }
            index_writer.commit().unwrap();
        }
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let mut in_values = Vec::new();

        for i in 1500..2500u64 {
            in_values.push(i);
        }
        for i in 9500..10500u64 {
            in_values.push(i);
        }

        let docs_in_set = TermInSetQuery::new_u64(numeric_field, &in_values);

        let count = searcher.search(&docs_in_set, &Count).unwrap();

        assert_eq!(count, 1500);
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use super::TermInSetQuery;
    use crate::collector::Count;
    use crate::query::{BooleanQuery, Occur, Query, TermQuery};
    use crate::schema::{Field, IndexRecordOption, Schema, INDEXED};
    use crate::{Index, IndexReader, Term};
    use test::Bencher;

    fn prepare_bench_example() -> (IndexReader, Field, Vec<u64>) {
        let n_docs = std::env::var("N_DOCS").expect("Set N_DOCS to control the n. of documents in the index");
        let n_docs = u64::from_str_radix(&n_docs, 10).expect("N_DOCS must be a positive integer");
        let in_set_size = std::env::var("IN_SET_SIZE").expect("Set IN_SET_SIZE to control the n. of values in the set to match");
        let in_set_size = u64::from_str_radix(&in_set_size, 10).expect("IN_SET_SIZE must be a positive integer");

        let mut schema_builder = Schema::builder();
        let numeric_field = schema_builder.add_u64_field("id", INDEXED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(1, 6_000_000).unwrap();
            for x in 0u64..n_docs {
                index_writer.add_document(doc!(numeric_field => x));
            }
            index_writer.commit().unwrap();
        }
        let reader = index.reader().unwrap();

        let mut in_values = Vec::new();

        for i in 1500u64..in_set_size {
            in_values.push(i);
        }

        (reader, numeric_field, in_values)
    }

    #[bench]
    fn bench_simple_term_in_set_query(bench: &mut Bencher) {
        let (reader, numeric_field, in_values) = prepare_bench_example();

        let searcher = reader.searcher();

        let in_set_query = TermInSetQuery::new_u64(numeric_field, &in_values);

        bench.iter(|| {
            let _count = searcher.search(&in_set_query, &Count).unwrap();
        });
    }

    #[bench]
    fn bench_simple_term_in_set_query_big_bool(bench: &mut Bencher) {
        let (reader, numeric_field, in_values) = prepare_bench_example();

        let searcher = reader.searcher();

        let big_bool_query = BooleanQuery::from(
            in_values
                .iter()
                .map(|v| {
                    let boxed_query: Box<dyn Query> = Box::new(TermQuery::new(
                        Term::from_field_u64(numeric_field, *v),
                        IndexRecordOption::Basic,
                    ));

                    (Occur::Should, boxed_query)
                })
                .collect::<Vec<_>>(),
        );

        bench.iter(|| {
            let _count = searcher.search(&big_bool_query, &Count).unwrap();
        });
    }
}
