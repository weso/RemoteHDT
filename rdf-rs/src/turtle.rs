use rio_turtle::{TurtleError, TurtleParser};
use std::{fs::File, io::BufReader};

use super::Backend;

pub struct Turtle;

impl Turtle {
    pub fn new() -> Self {
        Turtle {}
    }
}

impl Backend<TurtleParser<BufReader<File>>, TurtleError> for Turtle {
    fn concrete_parser(&self, reader: BufReader<File>) -> TurtleParser<BufReader<File>> {
        TurtleParser::new(reader, None)
    }
}
