use rio_turtle::TurtleError;
use rio_turtle::TurtleParser;
use std::fs::File;
use std::io::BufReader;

use super::Backend;

type TurtleFileParser = TurtleParser<BufReader<File>>;

pub struct Turtle;

impl Backend<TurtleFileParser, TurtleError> for Turtle {
    fn concrete_parser(reader: BufReader<File>) -> TurtleFileParser {
        TurtleParser::new(reader, None)
    }
}
