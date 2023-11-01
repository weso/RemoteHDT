use ntriples::NTriples;
use rdf_xml::RdfXml;
use rio_api::formatter::TriplesFormatter;
use rio_api::parser::TriplesParser;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use turtle::Turtle;

mod ntriples;
mod rdf_xml;
mod turtle;

pub struct Graph {
    triples: HashMap<String, Vec<(String, String)>>,
    subjects: HashSet<String>,
    predicates: HashMap<String, usize>,
    objects: HashMap<String, usize>,
}

impl Graph {
    pub fn new() -> Self {
        Graph {
            triples: HashMap::<String, Vec<(String, String)>>::new(),
            subjects: HashSet::<String>::new(),
            predicates: HashMap::<String, usize>::new(),
            objects: HashMap::<String, usize>::new(),
        }
    }

    pub fn insert(&mut self, subject: String, predicate: String, object: String) {
        if self.subjects.contains(&subject) {
            self.triples
                .get_mut(&subject)
                .unwrap()
                .push((predicate.to_owned(), object.to_owned()));
        } else {
            self.triples.insert(
                subject.to_owned(),
                vec![(predicate.to_owned(), object.to_owned())],
            );
        }

        self.subjects.insert(subject);

        if !self.predicates.contains_key(&predicate) {
            self.predicates.insert(predicate, self.predicates.len());
        }
        if !self.objects.contains_key(&object) {
            self.objects.insert(object, self.objects.len());
        }
    }

    pub fn triples(&self) -> HashMap<String, Vec<(String, String)>> {
        self.triples.to_owned()
    }

    pub fn subjects(&self) -> Vec<String> {
        Vec::from_iter(self.subjects.to_owned())
    }

    pub fn predicates(&self) -> HashMap<String, usize> {
        self.predicates.to_owned()
    }

    pub fn objects(&self) -> HashMap<String, usize> {
        self.objects.to_owned()
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
