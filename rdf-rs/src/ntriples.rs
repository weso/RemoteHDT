use sophia::turtle::parser::nt::NTriplesParser;
use sophia::turtle::serializer::nt::NtSerializer;
use std::fs::File;
use std::io::BufWriter;

use super::Backend;

pub(crate) struct NTriples;

impl<'a> Backend<'a, NTriplesParser, NtSerializer<BufWriter<File>>> for NTriples {
    fn concrete_parser(&self) -> NTriplesParser {
        NTriplesParser {}
    }

    fn concrete_formatter(&self, writer: BufWriter<File>) -> NtSerializer<BufWriter<File>> {
        NtSerializer::new(writer)
    }
}
