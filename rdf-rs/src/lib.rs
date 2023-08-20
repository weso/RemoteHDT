use ntriples::NTriples;
use rdf_xml::RdfXml;
use sophia::{
    parser::TripleParser,
    serializer::TripleSerializer,
    term::{BoxTerm, Term},
    triple::stream::TripleSource,
};
use std::{
    collections::HashSet,
    fs::File,
    io::{BufReader, BufWriter},
};
use turtle::Turtle;

mod ntriples;
mod rdf_xml;
mod turtle;

pub struct RdfParser {
    pub graph: Vec<[BoxTerm; 3]>,
}

trait Backend<P: TripleParser<BufReader<File>>, F: TripleSerializer> {
    fn parse(&self, path: &str) -> Result<Vec<[BoxTerm; 3]>, String> {
        let mut graph: Vec<[BoxTerm; 3]> = vec![];

        let reader = BufReader::new(match File::open(path) {
            Ok(file) => file,
            Err(_) => return Err(String::from("Cannot open the file")),
        });

        match self
            .concrete_parser()
            .parse(reader)
            .add_to_graph(&mut graph)
        {
            Ok(_) => Ok(graph),
            Err(_) => Err(String::from("Error parsing the graph")),
        }
    }

    fn format(&self, path: &str, graph: Vec<[BoxTerm; 3]>) -> Result<(), String> {
        let file = File::create(path).unwrap();
        let writer = BufWriter::new(file);
        let mut formatter = self.concrete_formatter(writer);
        match formatter.serialize_graph(&graph) {
            Ok(_) => Ok(()),
            Err(_) => Err(String::from("Error serializing the graph")),
        }
    }

    fn concrete_parser(&self) -> P;
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

    pub fn extract(
        &self,
    ) -> (
        HashSet<Term<Box<str>>>,
        HashSet<Term<Box<str>>>,
        HashSet<Term<Box<str>>>,
    ) {
        let mut subjects = HashSet::<Term<Box<str>>>::new();
        let mut predicates = HashSet::<Term<Box<str>>>::new();
        let mut objects = HashSet::<Term<Box<str>>>::new();

        self.graph.iter().for_each(|triple| {
            subjects.insert(triple[0].to_owned());
            predicates.insert(triple[1].to_owned());
            objects.insert(triple[2].to_owned());
        });

        (subjects, predicates, objects)
    }
}
