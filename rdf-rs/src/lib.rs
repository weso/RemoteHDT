use ntriples::NTriples;
use rdf_xml::RdfXml;
use sophia::graph::inmem::sync::FastGraph;
use sophia::parser::TripleParser;
use sophia::triple::stream::TripleSource;
use std::fs::File;
use std::io::BufReader;
use turtle::Turtle;

mod ntriples;
mod rdf_xml;
mod turtle;
pub struct RdfParser {
    pub graph: FastGraph,
}

trait Backend<'a, P: TripleParser<BufReader<File>>> {
    fn parse(&self, path: &str) -> Result<FastGraph, String> {
        let mut graph = FastGraph::new();

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

    fn concrete_parser(&self) -> P;
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
