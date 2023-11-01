use ntriples::NTriples;
use rdf_xml::RdfXml;
use rio_api::formatter::TriplesFormatter;
use rio_api::parser::TriplesParser;
use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use turtle::Turtle;

mod ntriples;
mod rdf_xml;
mod turtle;

pub struct Graph {
    triples: Vec<SimpleTriple>,
    subjects: HashSet<String>,
    predicates: HashSet<String>,
    objects: HashSet<String>,
}

impl Graph {
    pub fn new() -> Self {
        Graph {
            triples: Vec::<SimpleTriple>::new(),
            subjects: HashSet::<String>::new(),
            predicates: HashSet::<String>::new(),
            objects: HashSet::<String>::new(),
        }
    }

    pub fn insert(&mut self, subject: String, predicate: String, object: String) {
        self.triples.push(SimpleTriple::new(
            subject.to_owned(),
            predicate.to_owned(),
            object.to_owned(),
        ));
        self.subjects.insert(subject);
        self.predicates.insert(predicate);
        self.objects.insert(object);
    }

    pub fn triples(&self) -> Vec<SimpleTriple> {
        self.triples.to_owned()
    }

    pub fn subjects(&self) -> Vec<String> {
        Vec::from_iter(self.subjects.to_owned())
    }

    pub fn predicates(&self) -> Vec<String> {
        Vec::from_iter(self.predicates.to_owned())
    }

    pub fn objects(&self) -> Vec<String> {
        Vec::from_iter(self.objects.to_owned())
    }
}

#[derive(Clone)]
pub struct SimpleTriple {
    pub subject: String,
    pub predicate: String,
    pub object: String,
}

impl SimpleTriple {
    pub fn new(subject: String, predicate: String, object: String) -> Self {
        SimpleTriple {
            subject,
            predicate,
            object,
        }
    }
}

pub struct RdfParser {
    pub graph: Graph,
}

trait Backend<'a, P: TriplesParser, F: TriplesFormatter> {
    fn parse(&self, path: &str) -> Result<Graph, String> {
        let mut graph = Graph::new();

        let reader = BufReader::new(match File::open(path) {
            Ok(file) => file,
            Err(_) => return Err(String::from("Cannot open the file")),
        });

        match self.concrete_parser(reader).parse_all(&mut |t| {
            graph.insert(
                t.subject.to_string(),
                t.predicate.to_string(),
                t.object.to_string(),
            );
            Ok(()) as Result<(), Box<dyn Error>>
        }) {
            Ok(_) => Ok(graph),
            Err(_) => Err(String::from("Error parsing the graph")),
        }
    }

    fn concrete_parser(&self, reader: BufReader<File>) -> P;
    fn concrete_formatter(&self, writer: BufWriter<File>) -> F;
}

impl RdfParser {
    pub fn new(path: &str) -> Result<Self, String> {
        Ok(RdfParser {
            graph: match path.split('.').last() {
                Some("nt") => match NTriples.parse(path) {
                    Ok(graph) => graph,
                    Err(_) => return Err(String::from("Error loading the NTriples dump")),
                },
                Some("ttl") => match Turtle.parse(path) {
                    Ok(graph) => graph,
                    Err(_) => return Err(String::from("Error loading the Turtle dump")),
                },
                Some("rdf") => match RdfXml.parse(path) {
                    Ok(graph) => graph,
                    Err(_) => return Err(String::from("Error loading the RDF/XML dump")),
                },
                _ => return Err(String::from("Not supported format for loading the dump")),
            },
        })
    }
}
