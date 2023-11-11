use std::collections::HashSet;

use fcsd::Set;
use rayon::prelude::{IntoParallelRefIterator, ParallelBridge, ParallelIterator};
use serde_json::Value;
use sophia::term::ArcTerm;

pub fn term_to_value<'a>(terms: Set) -> Value {
    terms
        .iter()
        .par_bridge()
        .map(|(_, term)| std::str::from_utf8(&term).unwrap().to_string())
        .collect::<Vec<_>>()
        .into()
}

pub fn value_to_term<'a>(value: &'a Value) -> Vec<String> {
    let mut terms = value
        .as_array()
        .unwrap()
        .iter()
        .map(|term| term.as_str().unwrap().to_string())
        .collect::<Vec<String>>();
    terms.sort();
    terms
}

pub fn hash_to_set(terms: HashSet<ArcTerm>) -> Vec<String> {
    let mut vec = terms
        .par_iter()
        .map(|term| term.to_string())
        .collect::<Vec<_>>();
    vec.sort();
    vec
}
