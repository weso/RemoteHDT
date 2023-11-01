use std::fs::File;
use std::io::{BufReader, BufWriter};

use rio_xml::{RdfXmlFormatter, RdfXmlParser};

use super::Backend;

pub(crate) struct RdfXml;

impl<'a> Backend<'a, RdfXmlParser<BufReader<File>>, RdfXmlFormatter<BufWriter<File>>> for RdfXml {
    fn concrete_parser(&self, reader: BufReader<File>) -> RdfXmlParser<BufReader<File>> {
        RdfXmlParser::new(reader, None)
    }

    fn concrete_formatter(&self, writer: BufWriter<File>) -> RdfXmlFormatter<BufWriter<File>> {
        RdfXmlFormatter::new(writer).unwrap()
    }
}
