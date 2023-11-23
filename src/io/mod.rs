use rio_api::model::Triple;
use rio_api::parser::TriplesParser;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;

use crate::dictionary::Dictionary;
use crate::error::ParserError;

use self::ntriples::NTriples;
use self::rdf_xml::RdfXml;
use self::turtle::Turtle;

mod ntriples;
mod rdf_xml;
mod turtle;

pub type RdfParserResult = Result<(Graph, Dictionary), ParserError>;
pub type Graph = Vec<Vec<(u32, u32)>>;

trait Backend<T: TriplesParser, E: From<<T>::Error>> {
    fn parse(path: &str) -> RdfParserResult {
        // We create as many HashSets as fields we will be storing; that is, one
        // for the subjects, another for the predicates, and one for the objects.
        // The idea is that we will create a Dictionary matching every Term to
        // an integer value; thus, we will be able to store the Triples in a
        // more efficient manner
        let mut subjects = HashSet::new();
        let mut predicates = HashSet::new();
        let mut objects = HashSet::new();

        if let Err(err) = Self::parser_fn(path, &mut |triple: Triple| {
            {
                subjects.insert(triple.subject.to_string());
                predicates.insert(triple.predicate.to_string());
                objects.insert(triple.object.to_string());
            };
            Ok(())
        } as Result<(), E>)
        {
            return Err(ParserError::Dictionary(err));
        }

        let mut graph = vec![Vec::new(); subjects.len()];
        let dictionary = Dictionary::from_set_terms(subjects, predicates, objects);

        if let Err(err) = Self::parser_fn(path, &mut |triple: Triple| {
            {
                let sidx = dictionary.get_subject_idx_unchecked(&triple.subject.to_string());
                let pidx = dictionary.get_predicate_idx_unchecked(&triple.predicate.to_string());
                let oidx = dictionary.get_object_idx_unchecked(&triple.object.to_string());
                graph
                    .get_mut(sidx)
                    .unwrap()
                    .push((pidx as u32, oidx as u32))
            };
            Ok(())
        } as Result<(), E>)
        {
            return Err(ParserError::Graph(err));
        }

        Ok((graph, dictionary))
    }

    fn parser_fn(
        path: &str,
        on_triple: &mut impl FnMut(Triple<'_>) -> Result<(), E>,
    ) -> Result<(), String> {
        // We open a reader for the file that is requested to be read. The idea
        // is that we will iterate over the triples stored in a certain file
        let reader = BufReader::new(match File::open(path) {
            Ok(file) => file,
            Err(_) => return Err(String::from("Cannot open the file")),
        });

        // We create a parser that will be in charge of reading the file retrieving
        // the triples that are stored in the provided file
        let mut parser = Self::concrete_parser(reader);

        while !parser.is_end() {
            if parser.parse_step(on_triple).is_err() {
                // We skip the line if it is not a valid triple
                continue;
            }
        }

        Ok(())
    }

    fn concrete_parser(reader: BufReader<File>) -> T;
}

pub struct RdfParser;

impl RdfParser {
    pub fn parse(path: &str) -> RdfParserResult {
        match path.split('.').last() {
            Some("nt") => NTriples::parse(path),
            Some("ttl") => Turtle::parse(path),
            Some("rdf") => RdfXml::parse(path),
            Some(format) => Err(ParserError::NotSupportedFormat(format.to_string())),
            None => Err(ParserError::NoFormatProvided),
        }
    }
}
