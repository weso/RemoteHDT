use rio_api::model::Triple;
use rio_api::parser::TriplesParser;
use std::collections::HashSet;
use std::{fs::File, io::BufReader};

pub mod ntriples;
pub mod rdf_xml;
pub mod turtle;

// This is useful because we want to store framework independent Triples; that is,
// the Triple struct won't depend on any other RDF crate, such as `rio`. In case
// an external crate wants to make use of this struct, it won't need to import
// any additional Framework; what's more, the use of it is simplified, without
// having to deal with lifetimes or more complex data-types
#[derive(PartialEq)]
pub struct SimpleTriple {
    pub subject: String,
    pub predicate: String,
    pub object: String,
}

pub struct RDF {
    pub triples: Vec<SimpleTriple>,
}

pub trait Backend<T: TriplesParser, E: From<<T>::Error>> {
    fn load(&self, path: &str) -> Result<RDF, String> {
        let mut triples: Vec<SimpleTriple> = Vec::new();

        let reader = BufReader::new(match File::open(path) {
            Ok(file) => file,
            Err(_) => return Err(String::from("Cannot open the file")),
        });

        let mut parser = self.concrete_parser(reader);

        let mut on_triple = |triple: Triple| {
            {
                triples.push(SimpleTriple {
                    subject: triple.subject.to_string(),
                    predicate: triple.predicate.to_string(),
                    object: triple.object.to_string(),
                })
            };
            Ok(())
        } as Result<(), E>;

        while !parser.is_end() {
            if parser.parse_step(&mut on_triple).is_err() {
                // We skip the line if it is not a valid triple
                continue;
            }
        }

        Ok(RDF { triples })
    }

    fn concrete_parser(&self, reader: BufReader<File>) -> T;
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
