use std::{fs::File, io::BufReader};

use rio_api::parser::TriplesParser;
use rio_turtle::TurtleError;
use rio_turtle::TurtleParser;

use crate::RDF;

use super::Backend;

pub struct Turtle;

impl Backend for Turtle {
    fn load(path: &str) -> Result<RDF, String> {
        let mut triples: Vec<crate::Triple> = Vec::new();

        let reader = BufReader::new(match File::open(path) {
            Ok(file) => file,
            Err(_) => return Err(String::from("Cannot open the file")),
        });

        let mut parser = TurtleParser::new(reader, None);

        let mut on_triple = |triple: rio_api::model::Triple| {
            {
                triples.push(crate::Triple {
                    subject: triple.subject.to_string(),
                    predicate: triple.predicate.to_string(),
                    object: triple.object.to_string(),
                })
            };
            Ok(())
        } as Result<(), TurtleError>;

        while !parser.is_end() {
            if parser.parse_step(&mut on_triple).is_err() {
                // We skip the line if it is not a valid triple
                continue;
            }
        }

        Ok(RDF { triples })
    }
}