use ntriples::NTriples;
use rdf_xml::RdfXml;
use sophia::parser::TripleParser;
use sophia::serializer::TripleSerializer;
use sophia::term::Term;
use sophia::triple::stream::TripleSource;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use turtle::Turtle;

mod ntriples;
mod rdf_xml;
mod turtle;

pub type RdfGraph = Vec<[Term<String>; 3]>;

pub struct RdfParser {
    pub graph: RdfGraph,
}

pub struct RdfSerializer;

trait Backend<P: TripleParser<BufReader<File>>, F: TripleSerializer> {
    fn parse(&self, path: &str) -> Result<RdfGraph, String> {
        let mut graph: RdfGraph = vec![];

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

    fn format(&self, path: &str, graph: RdfGraph) -> Result<(), String> {
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
}

impl RdfSerializer {
    pub fn serialize(path: &str, graph: RdfGraph) -> Result<(), String> {
        match path.split('.').last() {
            Some("nt") => NTriples.format(path, graph),
            Some("ttl") => Turtle.format(path, graph),
            Some("rdf") => RdfXml.format(path, graph),
            _ => return Err(String::from("Not supported format for loading the dump")),
        }
    }
}
