use crate::query::{Query, Weight, Scorer, Explanation, ConstScorer, BitSetDocSet};
use crate::schema::{Field, Type, IndexRecordOption};
use crate::{Result, SegmentReader, SkipResult};
use crate::{Searcher, TantivyError, Term};
use std::collections::HashSet;
use std::sync::Arc;
use crate::common::BitSet;
use crate::query::explanation::does_not_match;
use crate::termdict::TermDictionaryBuilder;

#[derive(Clone, Debug)]
pub struct TermInSetQuery {
    field: Field,
    value_type: Type,
    values: Arc<HashSet<Vec<u8>>>,
}

impl TermInSetQuery {
    pub fn new_u64(field: Field, values: &[u64]) -> Self {
        let mut term = Term::from_field_u64(field, 42);

        let mut sorted_values = values.to_owned();
        sorted_values.sort();

        TermInSetQuery {
            field,
            value_type: Type::U64,
            values: Arc::new(
                sorted_values
                    .iter()
                    .map(|value| {
                        term.set_u64(*value);
                        term.value_bytes().to_owned()
                    })
                    .collect(),
            ),
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
            values: self.values.clone(),
        }))
    }
}

struct SetMembershipWeight {
    field: Field,
    values: Arc<HashSet<Vec<u8>>>,
}

impl Weight for SetMembershipWeight {
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<dyn Scorer>> {
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);

        let inverted_index = reader.inverted_index(self.field);
        let term_dict = inverted_index.terms();

        let field_type = reader.schema().get_field_entry(self.field).field_type();


        for value in self.values.iter() {
            if let Some(term_info) = term_dict.get(value) {
                let mut block_segment_postings = inverted_index
                    .read_block_postings_from_terminfo(&term_info, IndexRecordOption::Basic);
                while block_segment_postings.advance() {
                    for &doc in block_segment_postings.docs() {
                        doc_bitset.insert(doc);
                    }
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
    use crate::schema::{Schema, INDEXED};
    use crate::Index;
    use crate::collector::Count;

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
    use crate::schema::{Schema, INDEXED};
    use crate::Index;
    use crate::collector::Count;
    use test::Bencher;

    #[bench]
    fn bench_simple_term_in_set_query(bench: &mut Bencher) {
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

        let docs_in_the_sixties = TermInSetQuery::new_u64(numeric_field, &in_values);

        bench.iter(|| {
            let count = searcher.search(&docs_in_the_sixties, &Count).unwrap();
        });
    }
}
