use sophia::parser::nt::NTriplesParser;

use super::Backend;

pub(crate) struct NTriples;

impl<'a> Backend<'a, NTriplesParser> for NTriples {
    fn concrete_parser(&self) -> NTriplesParser {
        NTriplesParser {}
    }
}
