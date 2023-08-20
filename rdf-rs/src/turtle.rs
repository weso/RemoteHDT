use sophia::parser::turtle::TurtleParser;
use sophia::serializer::turtle::TurtleSerializer;
use std::fs::File;
use std::io::BufWriter;

use super::Backend;

pub(crate) struct Turtle;

impl Backend<TurtleParser, TurtleSerializer<BufWriter<File>>> for Turtle {
    fn concrete_parser(&self) -> TurtleParser {
        TurtleParser { base: None }
    }

    fn concrete_formatter(&self, writer: BufWriter<File>) -> TurtleSerializer<BufWriter<File>> {
        TurtleSerializer::new(writer)
    }
}
