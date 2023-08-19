use rio_xml::{RdfXmlError, RdfXmlParser};
use std::{fs::File, io::BufReader};

use super::Backend;

pub(crate) struct RdfXml;

impl Backend<RdfXmlParser<BufReader<File>>, RdfXmlError> for RdfXml {
    fn concrete_parser(&self, reader: BufReader<File>) -> RdfXmlParser<BufReader<File>> {
        RdfXmlParser::new(reader, None)
    }
}
