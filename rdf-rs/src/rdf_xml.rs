use sophia::xml::parser::RdfXmlParser;
use sophia::xml::serializer::RdfXmlSerializer;
use std::fs::File;
use std::io::BufWriter;

use super::Backend;

pub(crate) struct RdfXml;

impl<'a> Backend<'a, RdfXmlParser, RdfXmlSerializer<BufWriter<File>>> for RdfXml {
    fn concrete_parser(&self) -> RdfXmlParser {
        RdfXmlParser { base: None }
    }

    fn concrete_formatter(&self, writer: BufWriter<File>) -> RdfXmlSerializer<BufWriter<File>> {
        RdfXmlSerializer::new(writer)
    }
}
