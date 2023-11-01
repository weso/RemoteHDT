use std::fs::File;
use std::io::{BufReader, BufWriter};

use rio_turtle::{NTriplesFormatter, NTriplesParser};

use super::Backend;

pub(crate) struct NTriples;

impl<'a> Backend<'a, NTriplesParser<BufReader<File>>, NTriplesFormatter<BufWriter<File>>>
    for NTriples
{
    fn concrete_parser(&self, reader: BufReader<File>) -> NTriplesParser<BufReader<File>> {
        NTriplesParser::new(reader)
    }

    fn concrete_formatter(&self, writer: BufWriter<File>) -> NTriplesFormatter<BufWriter<File>> {
        NTriplesFormatter::new(writer)
    }
}
