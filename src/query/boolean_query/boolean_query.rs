use super::boolean_weight::BooleanWeight;
use crate::common::BitSet;
use crate::query::explanation::does_not_match;
use crate::query::Query;
use crate::query::TermQuery;
use crate::query::Weight;
use crate::query::{BitSetDocSet, ConstScorer, Explanation, Occur, Scorer};
use crate::schema::Term;
use crate::schema::{Field, IndexRecordOption};
use crate::Searcher;
use crate::{Result, SegmentReader, SkipResult};
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

/// The boolean query combines a set of queries
///
/// The documents matched by the boolean query are
/// those which
/// * match all of the sub queries associated with the
/// `Must` occurence
/// * match none of the sub queries associated with the
/// `MustNot` occurence.
/// * match at least one of the subqueries that is not
/// a `MustNot` occurence.
#[derive(Debug)]
pub struct BooleanQuery {
    subqueries: Vec<(Occur, Box<dyn Query>)>,
}

impl Clone for BooleanQuery {
    fn clone(&self) -> Self {
        self.subqueries
            .iter()
            .map(|(occur, subquery)| (*occur, subquery.box_clone()))
            .collect::<Vec<_>>()
            .into()
    }
}

impl From<Vec<(Occur, Box<dyn Query>)>> for BooleanQuery {
    fn from(subqueries: Vec<(Occur, Box<dyn Query>)>) -> BooleanQuery {
        let q = BooleanQuery { subqueries };
        trace!("trying to optimize");
        let optimized_q = q.optimize();
        match optimized_q {
            Cow::Borrowed(_) => {
                trace!("not optimized");
                q
            }
            Cow::Owned(new_q) => {
                trace!("optimized");
                new_q
            }
        }
    }
}

impl Query for BooleanQuery {
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> Result<Box<dyn Weight>> {
        let sub_weights = self
            .subqueries
            .iter()
            .map(|&(ref occur, ref subquery)| {
                Ok((*occur, subquery.weight(searcher, scoring_enabled)?))
            })
            .collect::<Result<_>>()?;
        Ok(Box::new(BooleanWeight::new(sub_weights, scoring_enabled)))
    }

    fn query_terms(&self, term_set: &mut BTreeSet<Term>) {
        for (_occur, subquery) in &self.subqueries {
            subquery.query_terms(term_set);
        }
    }
}

impl BooleanQuery {
    /// Helper method to create a boolean query matching a given list of terms.
    /// The resulting query is a disjunction of the terms.
    pub fn new_multiterms_query(terms: Vec<Term>) -> BooleanQuery {
        let occur_term_queries: Vec<(Occur, Box<dyn Query>)> = terms
            .into_iter()
            .map(|term| {
                let term_query: Box<dyn Query> =
                    Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
                (Occur::Should, term_query)
            })
            .collect();
        BooleanQuery::from(occur_term_queries)
    }

    /// Deconstructed view of the clauses making up this query.
    pub fn clauses(&self) -> &[(Occur, Box<dyn Query>)] {
        &self.subqueries[..]
    }

    /// Returns a rewritten version of the query, if possible.
    pub fn optimize(&self) -> Cow<BooleanQuery> {
        for (matching_i, subquery) in self.subqueries.iter().enumerate() {
            match subquery {
                (Occur::Must, subquery) => {
                    if let Some(boolean_subquery) = subquery.downcast_ref::<BooleanQuery>() {
                        if boolean_subquery.subqueries.len() > 8
                            && can_be_represented_as_set_membership(&boolean_subquery.subqueries)
                        {
                            trace!(
                                "*** pattern detected (len: {})",
                                boolean_subquery.subqueries.len()
                            );

                            let mut new_subqueries = self
                                .subqueries
                                .iter()
                                .enumerate()
                                .filter(|&(i, _)| i != matching_i)
                                .map(|s| s.1)
                                .map(|(occur, q)| (*occur, q.box_clone()))
                                .collect::<Vec<_>>();

                            let optimizable_subqueries = boolean_subquery
                                .subqueries
                                .iter()
                                .map(|q| {
                                    *q.1.box_clone()
                                        .downcast::<TermQuery>()
                                        .expect("we just checked it in can_be_represented_as_set_membership()")
                                })
                                .collect::<Vec<_>>();

                            let materialized_query =
                                MaterializedTermQuery::from(optimizable_subqueries);

                            let mut out: Vec<(Occur, Box<dyn Query>)> = Vec::new();

                            out.push((Occur::Must, Box::new(materialized_query)));

                            out.append(&mut new_subqueries);

                            return Cow::Owned(BooleanQuery::from(out));
                        }
                    }
                }
                _ => {}
            }
        }
        Cow::Borrowed(self)
    }
}

fn can_be_represented_as_set_membership(weights: &Vec<(Occur, Box<dyn Query>)>) -> bool {
    let all_occurs_are_should = weights
        .iter()
        .map(|w| w.0)
        .all(|occur| occur.eq(&Occur::Should));

    if !all_occurs_are_should {
        return false;
    }

    let is_all_term_queries = weights
        .iter()
        .map(|w| &w.1)
        .all(|query| query.is::<TermQuery>());

    if !is_all_term_queries {
        return false;
    }

    let is_match_only =
        weights
            .iter()
            .map(|w| &w.1)
            .all(|query| match query.downcast_ref::<TermQuery>() {
                Some(term_query) => term_query.is_match_only(),
                _ => false,
            });

    if !is_match_only {
        return false;
    }

    return true;
}

type FieldValue = Vec<u8>;

#[derive(Clone, Debug)]
struct MaterializedTermQuery {
    values: Arc<HashMap<Field, Vec<FieldValue>>>,
}

impl From<Vec<TermQuery>> for MaterializedTermQuery {
    fn from(term_queries: Vec<TermQuery>) -> Self {
        let mut values = HashMap::new();

        for term_query in term_queries {
            values
                .entry(term_query.term().field())
                .or_insert_with(|| Vec::new())
                .push(term_query.term().value_bytes().to_owned())
        }

        MaterializedTermQuery {
            values: Arc::new(values),
        }
    }
}

impl Query for MaterializedTermQuery {
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> Result<Box<dyn Weight>> {
        // TODO: validation, same as RangeQuery except that we check for more than one field.

        Ok(Box::new(MaterializedTermQueryWeight {
            values: self.values.clone(),
        }))
    }
}

struct MaterializedTermQueryWeight {
    values: Arc<HashMap<Field, Vec<FieldValue>>>,
}

impl Weight for MaterializedTermQueryWeight {
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<dyn Scorer>> {
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);

        for (field, values) in self.values.iter() {
            let inverted_index = reader.inverted_index(*field);
            let term_dict = inverted_index.terms();

            for value in values {
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
        }

        let doc_bitset = BitSetDocSet::from(doc_bitset);
        Ok(Box::new(ConstScorer::new(doc_bitset)))
    }

    fn explain(&self, reader: &SegmentReader, doc: u32) -> Result<Explanation> {
        let mut scorer = self.scorer(reader)?;
        if scorer.skip_next(doc) != SkipResult::Reached {
            return Err(does_not_match(doc));
        }
        Ok(Explanation::new("MaterializedTermQueryWeight", 1.0f32))
    }
}
