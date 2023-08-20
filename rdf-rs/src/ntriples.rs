use sophia::{parser::nt::NTriplesParser, serializer::nt::NtSerializer};
use std::{fs::File, io::BufWriter};

use super::Backend;

pub(crate) struct NTriples;

impl Backend<NTriplesParser, NtSerializer<BufWriter<File>>> for NTriples {
    fn concrete_parser(&self) -> NTriplesParser {
        NTriplesParser {}
    }

    fn concrete_formatter(&self, writer: BufWriter<File>) -> NtSerializer<BufWriter<File>> {
        NtSerializer::new(writer)
    }
}
