use oxigraph::io::GraphFormat;
use oxigraph::io::GraphParser;
use oxigraph::model::Graph;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;

use crate::storage::dictionary::Dictionary;

pub struct RdfParser {
    path: String,
    format: GraphFormat,
}

impl RdfParser {
    pub fn new(path: &str) -> Result<Self, String> {
        match path.split('.').last() {
            Some("nt") => Ok(RdfParser {
                path: path.to_string(),
                format: GraphFormat::NTriples,
            }),
            Some("ttl") => Ok(RdfParser {
                path: path.to_string(),
                format: GraphFormat::Turtle,
            }),
            Some("rdf") => Ok(RdfParser {
                path: path.to_string(),
                format: GraphFormat::RdfXml,
            }),
            _ => return Err(String::from("Not supported format for loading the dump")),
        }
    }

    pub fn parse(&self) -> Result<(Graph, Dictionary), String> {
        let mut graph = Graph::new();
        let mut subjects = HashSet::new();
        let mut predicates = HashSet::new();
        let mut objects = HashSet::new();

        let reader = BufReader::new(match File::open(self.path.clone()) {
            Ok(file) => file,
            Err(_) => return Err(String::from("Cannot open the file")),
        });

        let triples = match GraphParser::from_format(self.format).read_triples(reader) {
            Ok(iter) => iter,
            Err(_) => return Err(String::from("Error parsing the graph")),
        };

        for triple in triples {
            if let Ok(triple) = triple {
                graph.insert(&triple);
                subjects.insert(triple.subject.to_string());
                predicates.insert(triple.predicate.to_string());
                objects.insert(triple.object.to_string());
            }
        }

        Ok((
            graph,
            Dictionary::from_set_terms(subjects, predicates, objects),
        ))
    }
}
