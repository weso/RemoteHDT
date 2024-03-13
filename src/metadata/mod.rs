use std::default;
use std::path::PathBuf;
use std::str::FromStr;
use std::collections::HashSet;
use std::sync::Arc;

use serde_json::Map;
use zarrs::metadata;

use crate::io::CSVParser;
use crate::storage::layout;
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
use crate::metadata::params::MetadataDimensionality;

use fcsd::Set;
use zarrs::array::Array;
use zarrs::array::ArrayBuilder;
use zarrs::opendal::raw::oio::StreamExt;
use zarrs::opendal::services::Fs;
use zarrs::opendal::services::Http;
use zarrs::opendal::Operator;
use zarrs::storage::store::OpendalStore;
use self::structure::Structure;

use super::utils::hash_to_set;





pub mod structure;
pub mod params;

pub type MetadataResult<T> = Result<T, RemoteHDTError>;

const ARRAY_NAME: &str = "/group/RemoteHDT"; // TODO: parameterize this

pub struct Metadata<C> {
    serialization: Serialization,
    array: Option<Array<OpendalStore>>,
    dimensionality: MetadataDimensionality,
    structure: Box<dyn Structure<C>>,

}

impl<C> Metadata<C> {
    pub fn new( structure: impl Structure<C> + 'static, serialization: Serialization) -> Self {
        Metadata {
            serialization: serialization,
            array: None,
            dimensionality: Default::default(),
            structure: Box::new(structure),
        }
    }

    pub fn serialize<'a>(
        &mut self,
        store: Backend<'a>,
        metadata_path: &str,
        chunking_strategy: ChunkingStrategy,
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

        let metadata = match CSVParser::parse(metadata_path) {
            Ok(result) => {
                self.dimensionality = MetadataDimensionality::new(result.len(), fields.len());
                result
            }
            Err(_) => return Err(RemoteHDTError::CSVParse),
        };

  
       

        let arr = ArrayBuilder::new(
            self.structure.shape(&self.dimensionality),
            self.structure.data_type(),
            self.structure
                .chunk_shape(chunking_strategy, &self.dimensionality),
            self.structure.fill_value(),
        )
        .dimension_names(self.structure.dimension_names())
        .array_to_bytes_codec(self.structure.array_to_bytes_codec(&self.dimensionality)?)
        .attributes({
            let mut attributes = Map::new();
            attributes.insert("metadata_fields".into(), rdf_to_value(Set::new(fields).unwrap()));
            attributes
        })
        .build(store, ARRAY_NAME)?;

        arr.store_metadata()?;
        self.structure.serialize(arr, metadata)?;

        Ok(self)
    }

    pub fn load<'a>(
        &mut self,
        store: Backend<'a>,
        // threading_strategy: ThreadingStrategy, TODO: implement this
    ) -> MetadataResult<&mut Self> {
        let operator = match store {
            Backend::FileSystem(path) => {
                let mut builder = Fs::default();
                let path = PathBuf::from_str(path)?;

                match path.exists() {
                    false => return Err(RemoteHDTError::PathDoesNotExist),
                    true => {
                        let path = match path.into_os_string().into_string() {
                            Ok(string) => string,
                            Err(_) => return Err(RemoteHDTError::OsPathToString),
                        };
                        builder.root(&path);
                    }
                }

                Operator::new(builder)?.finish()
            }
            Backend::HTTP(path) => {
                let mut builder = Http::default();
                builder.endpoint(path);
                Operator::new(builder)?.finish()
            }
        };

        let store: Arc<OpendalStore> = Arc::new(OpendalStore::new(operator.blocking()));
        let arr = Array::new(store, ARRAY_NAME)?;
        let dictionary = self.structure.retrieve_attributes(&arr)?;
        self.dictionary = dictionary;
        self.reference_system = self.dictionary.get_reference_system();
        self.dimensionality = Dimensionality::new(&self.dictionary, &Graph::default());

        match self.serialization {
            Serialization::Zarr => self.array = Some(arr),
            Serialization::Sparse => {
                self.sparse_array = Some(self.layout.parse(&arr, &self.dimensionality)?)
            }
        }

        Ok(self)
    }
}
