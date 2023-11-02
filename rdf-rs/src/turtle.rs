use sophia::parser::turtle::TurtleParser;

use super::Backend;

pub(crate) struct Turtle;

impl<'a> Backend<'a, TurtleParser> for Turtle {
    fn concrete_parser(&self) -> TurtleParser {
        TurtleParser { base: None }
    }
}
