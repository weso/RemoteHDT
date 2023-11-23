use std::collections::HashSet;

use fcsd::Set;
use serde_json::Value;
use zarrs::array::Array;

pub fn rdf_to_value(terms: Set) -> Value {
    terms
        .iter()
        .map(|(_, term)| std::str::from_utf8(&term).unwrap().to_string())
        .collect::<Vec<_>>()
        .into()
}

pub fn value_to_term(value: &Value) -> Vec<String> {
    let mut terms = value
        .as_array()
        .unwrap()
        .iter()
        .map(|term| term.as_str().unwrap().to_string())
        .collect::<Vec<String>>();
    terms.sort();
    terms
}

pub fn hash_to_set(terms: HashSet<String>) -> Vec<String> {
    let mut vec = terms
        .iter()
        .map(|term| term.to_string())
        .collect::<Vec<_>>();
    vec.sort();
    vec
}

pub fn subjects_per_chunk<T>(arr: &Array<T>) -> u64 {
    match arr.chunk_grid().chunk_shape(&[0, 0], arr.shape()) {
        Ok(shape) => match shape {
            Some(chunk_shape) => chunk_shape[0],
            None => todo!(),
        },
        Err(_) => todo!(),
    }
}

pub fn objects_per_chunk<T>(arr: &Array<T>) -> u64 {
    match arr.chunk_grid().chunk_shape(&[0, 0], arr.shape()) {
        Ok(shape) => match shape {
            Some(chunk_shape) => chunk_shape[1],
            None => todo!(),
        },
        Err(_) => todo!(),
    }
}
