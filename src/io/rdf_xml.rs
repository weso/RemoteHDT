use rio_xml::RdfXmlError;
use rio_xml::RdfXmlParser;
use std::fs::File;
use std::io::BufReader;

use super::Backend;

type RdfXmlFileParser = RdfXmlParser<BufReader<File>>;

pub struct RdfXml;

impl Backend<RdfXmlFileParser, RdfXmlError> for RdfXml {
    fn concrete_parser(reader: BufReader<File>) -> RdfXmlFileParser {
        RdfXmlParser::new(reader, None)
    }
}
