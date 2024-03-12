use rio_api::model::Triple;
use rio_api::parser::TriplesParser;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::error::Error;
use csv::ReaderBuilder;

use crate::dictionary::Dictionary;
use crate::error::ParserError;
use crate::storage::params::ReferenceSystem;

use self::ntriples::NTriples;
use self::rdf_xml::RdfXml;
use self::turtle::Turtle;

mod ntriples;
mod rdf_xml;
mod turtle;

pub type RdfParserResult = Result<(Graph, Dictionary), ParserError>;
pub type Graph = Vec<Vec<(u32, u32)>>;

trait Backend<T: TriplesParser, E: From<<T>::Error>> {
    fn parse(path: &str, reference_system: &ReferenceSystem) -> RdfParserResult {
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

        let mut graph = vec![
            Vec::new();
            match reference_system {
                ReferenceSystem::SPO | ReferenceSystem::SOP => subjects.len(),
                ReferenceSystem::PSO | ReferenceSystem::POS => predicates.len(),
                ReferenceSystem::OSP | ReferenceSystem::OPS => objects.len(),
            }
        ];
        let dictionary =
            Dictionary::from_set_terms(reference_system.to_owned(), subjects, predicates, objects);

        if let Err(err) = Self::parser_fn(path, &mut |triple: Triple| {
            {
                let sidx = dictionary.get_subject_idx_unchecked(&triple.subject.to_string());
                let pidx = dictionary.get_predicate_idx_unchecked(&triple.predicate.to_string());
                let oidx = dictionary.get_object_idx_unchecked(&triple.object.to_string());

                match reference_system {
                    ReferenceSystem::SPO => {
                        if let Some(subject) = graph.get_mut(sidx) {
                            subject.push((pidx as u32, oidx as u32))
                        }
                    }
                    ReferenceSystem::SOP => {
                        if let Some(subject) = graph.get_mut(sidx) {
                            subject.push((oidx as u32, pidx as u32))
                        }
                    }
                    ReferenceSystem::PSO => {
                        if let Some(predicate) = graph.get_mut(pidx) {
                            predicate.push((sidx as u32, oidx as u32))
                        }
                    }
                    ReferenceSystem::POS => {
                        if let Some(predicate) = graph.get_mut(pidx) {
                            predicate.push((oidx as u32, sidx as u32))
                        }
                    }
                    ReferenceSystem::OPS => {
                        if let Some(object) = graph.get_mut(oidx) {
                            object.push((pidx as u32, sidx as u32))
                        }
                    }
                    ReferenceSystem::OSP => {
                        if let Some(object) = graph.get_mut(oidx) {
                            object.push((sidx as u32, pidx as u32))
                        }
                    }
                }
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
pub struct CSVParser;

impl RdfParser {
    pub fn parse(path: &str, reference_system: &ReferenceSystem) -> RdfParserResult {
        match path.split('.').last() {
            Some("nt") => NTriples::parse(path, reference_system),
            Some("ttl") => Turtle::parse(path, reference_system),
            Some("rdf") => RdfXml::parse(path, reference_system),
            Some(format) => Err(ParserError::NotSupportedFormat(format.to_string())),
            None => Err(ParserError::NoFormatProvided),
        }
    }
}

impl CSVParser {
    pub fn parse(filename: &str) -> Result<Vec<Vec<u32>>, Box<dyn Error>> {
        let file = File::open(filename)?;
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);
    
        let mut records: Vec<Vec<u32>> = Vec::new();
    
        for result in reader.records() {
            let record = result?;
            let values: Vec<u32> = record.iter().map(|s| s.parse::<u32>().unwrap()).collect();
            
            records.push(values);
        }
    
        Ok(records)
    }
}
