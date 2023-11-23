use rio_turtle::NTriplesParser;
use rio_turtle::TurtleError;
use std::fs::File;
use std::io::BufReader;

use super::Backend;

type NTriplesFileParser = NTriplesParser<BufReader<File>>;

pub struct NTriples;

impl Backend<NTriplesFileParser, TurtleError> for NTriples {
    fn concrete_parser(reader: BufReader<File>) -> NTriplesFileParser {
        NTriplesParser::new(reader)
    }
}
