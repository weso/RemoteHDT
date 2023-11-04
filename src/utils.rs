use std::collections::HashSet;

use fcsd::Set;
use rayon::prelude::{IntoParallelRefIterator, ParallelBridge, ParallelIterator};
use serde_json::Value;
use sophia::term::ArcTerm;

use crate::remote_hdt::ZarrArray;

pub fn print_matrix(matrix: ZarrArray) {
    if matrix.nrows() > 100 || matrix.ncols() > 100 {
        println!("{:?}", matrix.values());
        return;
    }

    let separator = format!("{}+", "+----".repeat(matrix.ncols()));

    println!("{:?}", matrix);

    matrix.row_iter().for_each(|row| {
        print!("{}\n|", separator);
        for i in 0..row.ncols() {
            match row.get_entry(i) {
                Some(predicate) => print!(" {:^2} |", predicate.into_value()),
                None => print!("{}", 0),
            }
        }
        println!()
    });

    println!("{}", separator);
}

pub fn term_to_value<'a>(terms: Set) -> Value {
    terms
        .iter()
        .par_bridge()
        .map(|(_, term)| std::str::from_utf8(&term).unwrap().to_string())
        .collect::<Vec<_>>()
        .into()
}

pub fn value_to_term<'a>(value: &'a Value) -> Vec<&'a str> {
    let mut terms = value
        .as_array()
        .unwrap()
        .iter()
        .map(|term| term.as_str().unwrap())
        .collect::<Vec<&str>>();
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
