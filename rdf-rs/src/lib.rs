use ntriples::NTriples;
use rdf_xml::RdfXml;
use rio_api::model::Triple;
use rio_api::parser::TriplesParser;
use std::collections::HashSet;
use std::{fs::File, io::BufReader};
use turtle::Turtle;

mod ntriples;
mod rdf_xml;
mod turtle;

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

trait Backend<T: TriplesParser, E: From<<T>::Error>> {
    fn parse(&self, path: &str) -> Result<Vec<SimpleTriple>, String> {
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

        Ok(triples)
    }

    fn concrete_parser(&self, reader: BufReader<File>) -> T;
}

impl RDF {
    pub fn new(path: &str) -> Result<Self, String> {
        let triples = match path.split('.').last() {
            Some("nt") => match NTriples.parse(path) {
                Ok(triples) => triples,
                Err(_) => return Err(String::from("Error loading the NTriples dump")),
            },
            Some("ttl") => match Turtle.parse(path) {
                Ok(triples) => triples,
                Err(_) => return Err(String::from("Error loading the Turtle dump")),
            },
            Some("rdf") => match RdfXml.parse(path) {
                Ok(triples) => triples,
                Err(_) => return Err(String::from("Error loading the RDF/XML dump")),
            },
            _ => return Err(String::from("Not supported format for loading the dump")),
        };

        Ok(RDF { triples })
    }

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
