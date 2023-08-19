use rio_turtle::{NTriplesParser, TurtleError};
use std::{fs::File, io::BufReader};

use super::Backend;

pub struct NTriples;

impl NTriples {
    pub fn new() -> Self {
        NTriples {}
    }
}

impl Backend<NTriplesParser<BufReader<File>>, TurtleError> for NTriples {
    fn concrete_parser(&self, reader: BufReader<File>) -> NTriplesParser<BufReader<File>> {
        NTriplesParser::new(reader)
    }
}
