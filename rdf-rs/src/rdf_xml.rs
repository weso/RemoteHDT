use sophia::parser::xml::RdfXmlParser;

use super::Backend;

pub(crate) struct RdfXml;

impl<'a> Backend<'a, RdfXmlParser> for RdfXml {
    fn concrete_parser(&self) -> RdfXmlParser {
        RdfXmlParser { base: None }
    }
}
