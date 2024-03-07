use std::default;
use std::path::PathBuf;
use std::str::FromStr;
use std::collections::HashSet;
use std::sync::Arc;

use serde_json::Map;

use crate::storage::layout;
use crate::storage::layout::metadata::MetadataLayout;
use crate::storage::layout::tabular::TabularLayout;
use crate::utils::rdf_to_value;

use crate::dictionary::Dictionary;
use crate::io::Graph;
use crate::io::RdfParser;

use crate::error::RemoteHDTError;
use crate::storage::layout::Layout;
use crate::storage::params::Backend;
use crate::storage::params::Dimensionality;
use crate::storage::params::ReferenceSystem;
use crate::storage::params::Serialization;
use crate::storage::params::ChunkingStrategy;

use fcsd::Set;
use zarrs::array::Array;
use zarrs::array::ArrayBuilder;
use zarrs::opendal::raw::oio::StreamExt;
use zarrs::opendal::services::Fs;
use zarrs::opendal::services::Http;
use zarrs::opendal::Operator;
use zarrs::storage::store::OpendalStore;
use super::utils::hash_to_set;


pub type MetadataResult<T> = Result<T, RemoteHDTError>;
pub mod structure;

const ARRAY_NAME: &str = "/group/RemoteHDT"; // TODO: parameterize this

pub struct Metadata<C> {
    flatten_graph: Vec<(String)>,
    serialization: Serialization,
    dictionary: Dictionary,
    array: Option<Array<OpendalStore>>,
    dimensionality: Dimensionality,
    layout: Box<dyn Layout<C>>,
}

impl<C> Metadata<C> {
    pub fn new( layout: impl Layout<C> + 'static, serialization: Serialization) -> Self {
        Metadata {
            flatten_graph: Vec::<String>::default(),
            serialization: serialization,
            dictionary: Dictionary::default(),
            array: None,
            dimensionality: Default::default(),
            layout: Box::new(layout),
        }
    }

    pub fn serialize<'a>(
        &mut self,
        store: Backend<'a>,
        rdf_path: &str,
        chunking_strategy: ChunkingStrategy,
        reference_system: ReferenceSystem,
        
        metadata_path: &str,
        fields: Vec<&str>,
    ) -> MetadataResult<&mut Self> {

        let operator = match store {
            Backend::FileSystem(path) => {
                let mut builder = Fs::default();
                let path = PathBuf::from_str(path)?;

                match path.exists() {
                    true => return Err(RemoteHDTError::PathExists),
                    false => {
                        let path = match path.into_os_string().into_string() {
                            Ok(string) => string,
                            Err(_) => return Err(RemoteHDTError::OsPathToString),
                        };
                        builder.root(&path);
                    }
                }

                Operator::new(builder)?.finish()
            }
            Backend::HTTP(_) => return Err(RemoteHDTError::ReadOnlyBackend),
        };


        // 2. We can create the FileSystemStore appropiately
        let store = Arc::new(OpendalStore::new(operator.blocking()));

        let graph = match RdfParser::parse(rdf_path, &reference_system) {
            Ok((graph, dictionary)) => {
                self.dictionary = dictionary;
                self.dimensionality = Dimensionality::new(&self.dictionary, &graph);
                graph
            }
            Err(_) => return Err(RemoteHDTError::RdfParse),
        };

        //Flatten the graph into triples
        let mut count = 0;
        for i in graph.iter() {
            for j in i.iter() {
                self.flatten_graph.push(format!["{};{};{}",count, j.0, j.1])
            }
            count += 1;
        }

        //TODO: change the implementation so it is only done here the flatten
        let triples:HashSet<_> = self.flatten_graph.clone().into_iter().collect();
        let subjects = self.dictionary.subjects();
        let predicates = self.dictionary.predicates();
        let objects = self.dictionary.objects();

        let arr = ArrayBuilder::new(
            self.layout.shape(&self.dimensionality),
            self.layout.data_type(),
            self.layout
                .chunk_shape(chunking_strategy, &self.dimensionality),
            self.layout.fill_value(),
        )
        .dimension_names(self.layout.dimension_names(&reference_system))
        .array_to_bytes_codec(self.layout.array_to_bytes_codec(&self.dimensionality)?)
        .attributes({
            let mut attributes = Map::new();
            attributes.insert("triples".into(), rdf_to_value(Set::new(hash_to_set(triples)).unwrap()));
            attributes.insert("subjects".into(), rdf_to_value(subjects));
            attributes.insert("predicates".into(), rdf_to_value(predicates));
            attributes.insert("objects".into(), rdf_to_value(objects));
            attributes.insert("reference_system".into(), reference_system.as_ref().into());
            attributes
        })
        .build(store, ARRAY_NAME)?;

        arr.store_metadata()?;
        self.layout.serialize(arr, graph)?;

        Ok(self)
    }
}
