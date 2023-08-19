use rio_turtle::{TurtleError, TurtleParser};
use std::{fs::File, io::BufReader};

use super::Backend;

#[derive(Default)]
pub struct Turtle;

impl Backend<TurtleParser<BufReader<File>>, TurtleError> for Turtle {
    fn concrete_parser(&self, reader: BufReader<File>) -> TurtleParser<BufReader<File>> {
        TurtleParser::new(reader, None)
    }
}
