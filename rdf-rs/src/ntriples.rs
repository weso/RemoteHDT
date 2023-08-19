use rio_turtle::{NTriplesParser, TurtleError};
use std::{fs::File, io::BufReader};

use super::Backend;

#[derive(Default)]
pub struct NTriples;

impl Backend<NTriplesParser<BufReader<File>>, TurtleError> for NTriples {
    fn concrete_parser(&self, reader: BufReader<File>) -> NTriplesParser<BufReader<File>> {
        NTriplesParser::new(reader)
    }
}
