use std::fs::File;
use std::io::{BufReader, BufWriter};

use rio_turtle::{TurtleFormatter, TurtleParser};

use super::Backend;

pub(crate) struct Turtle;

impl<'a> Backend<'a, TurtleParser<BufReader<File>>, TurtleFormatter<BufWriter<File>>> for Turtle {
    fn concrete_parser(&self, reader: BufReader<File>) -> TurtleParser<BufReader<File>> {
        TurtleParser::new(reader, None)
    }

    fn concrete_formatter(&self, writer: BufWriter<File>) -> TurtleFormatter<BufWriter<File>> {
        TurtleFormatter::new(writer)
    }
}
