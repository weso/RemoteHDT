use oxigraph::io::read::TripleReader;
use oxigraph::io::GraphFormat;
use oxigraph::io::GraphParser;
use std::fs::File;
use std::io::BufReader;

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

    pub fn parse(&self) -> Result<TripleReader<BufReader<File>>, String> {
        let reader = BufReader::new(match File::open(self.path.clone()) {
            Ok(file) => file,
            Err(_) => return Err(String::from("Cannot open the file")),
        });

        match GraphParser::from_format(self.format).read_triples(reader) {
            Ok(iter) => Ok(iter),
            Err(_) => Err(String::from("Error parsing the graph")),
        }
    }
}
