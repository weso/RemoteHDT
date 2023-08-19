use std::collections::HashSet;

pub mod ntriples;
pub mod rdf_xml;
pub mod turtle;

// This is useful because we want to store framework independent Triples; that is,
// the Triple struct won't depend on any other RDF crate, such as `rio`. In case
// an external crate wants to make use of this struct, it won't need to import
// any additional Framework; what's more, the use of it is simplified, without
// having to deal with lifetimes or more complex data-types
#[derive(PartialEq)]
pub struct Triple {
    pub subject: String,
    pub predicate: String,
    pub object: String,
}

pub struct RDF {
    pub triples: Vec<Triple>,
}

pub trait Backend {
    fn load(path: &str) -> Result<RDF, String>;
}

impl RDF {
    pub fn extract(&self) -> (HashSet<String>, HashSet<String>, HashSet<String>) {
        let mut subjects = HashSet::<String>::new();
        let mut predicates = HashSet::<String>::new();
        let mut objects = HashSet::<String>::new();

        self.triples.iter().for_each(|triple| {
            subjects.insert(triple.subject.to_string());
            predicates.insert(triple.predicate.to_string());
            objects.insert(triple.object.to_string());
        });

        (subjects, predicates, objects)
    }
}
