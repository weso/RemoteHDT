use sophia::turtle::parser::turtle::TurtleParser;
use sophia::turtle::serializer::turtle::TurtleSerializer;
use std::fs::File;
use std::io::BufWriter;

use super::Backend;

pub(crate) struct Turtle;

impl<'a> Backend<'a, TurtleParser, TurtleSerializer<BufWriter<File>>> for Turtle {
    fn concrete_parser(&self) -> TurtleParser {
        TurtleParser { base: None }
    }

    fn concrete_formatter(&self, writer: BufWriter<File>) -> TurtleSerializer<BufWriter<File>> {
        TurtleSerializer::new(writer)
    }
}
