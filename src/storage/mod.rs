use serde_json::Map;
use sprs::CsMat;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use zarrs::array::Array;
use zarrs::array::ArrayBuilder;
use zarrs::group::GroupBuilder;
use zarrs::opendal::services::Fs;
use zarrs::opendal::services::Http;
use zarrs::opendal::Operator;
use zarrs::storage::store::OpendalStore;

use crate::dictionary::Dictionary;
use crate::error::RemoteHDTError;
use crate::io::Graph;
use crate::io::RdfParser;
use crate::utils::rdf_to_value;

use self::layout::Layout;
use self::params::Backend;
use self::params::ChunkingStrategy;
use self::params::Dimensionality;
use self::params::ReferenceSystem;
use self::params::Serialization;

pub mod layout;
pub mod ops;
pub mod params;

pub type ZarrArray = CsMat<usize>;
type AtomicZarrType = AtomicU32;
pub type StorageResult<T> = Result<T, RemoteHDTError>;

const ARRAY_NAME: &str = "/group/RemoteHDT"; // TODO: parameterize this

pub struct Storage<C> {
    dictionary: Dictionary,
    dimensionality: Dimensionality,
    layout: Box<dyn Layout<C>>,
    serialization: Serialization,
    reference_system: ReferenceSystem,
    array: Option<Array<OpendalStore>>,
    sparse_array: Option<ZarrArray>,
}

impl<C> Storage<C> {
    pub fn new(layout: impl Layout<C> + 'static, serialization: Serialization) -> Self {
        Storage {
            dictionary: Default::default(),
            dimensionality: Default::default(),
            layout: Box::new(layout),
            serialization,
            reference_system: ReferenceSystem::SPO,
            array: None,
            sparse_array: None,
        }
    }

    pub fn get_dictionary(&self) -> Dictionary {
        self.dictionary.to_owned()
    }

    pub fn get_sparse_array(&self) -> Option<ZarrArray> {
        self.sparse_array.to_owned()
    }

    /// # Errors
    /// Returns [`PathExistsError`] if the provided path already exists; that is,
    /// the user is trying to store the RDF dataset in an occupied storage. This
    /// is due to the fact that the user may incur in an undefined state.
    pub fn serialize<'a>(
        &mut self,
        store: Backend<'a>,
        rdf_path: &'a str,
        chunking_strategy: ChunkingStrategy,
        reference_system: ReferenceSystem,
        // threading_strategy: ThreadingStrategy, TODO: implement this
    ) -> StorageResult<&mut Self> {
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

        // Create a group and write metadata to filesystem
        let group = GroupBuilder::new().build(store.clone(), "/group")?;

        let _ = group.store_metadata()?;

        // TODO: rayon::ThreadPoolBuilder::new()
        //     .num_threads(1)
        //     .build_global()
        //     .unwrap();

        // 3. Import the RDF dump using `rdf-rs`
        let graph = match RdfParser::parse(rdf_path, &reference_system) {
            Ok((graph, dictionary)) => {
                self.dictionary = dictionary;
                self.dimensionality = Dimensionality::new(&self.dictionary, &graph);
                graph
            }
            Err(_) => return Err(RemoteHDTError::RdfParse),
        };

        // 4. Build the structure of the Array; as such, several parameters of it are
        // tweaked. Namely, the size of the array, the size of the chunks, the name
        // of the different dimensions and the default values
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

    pub fn load<'a>(
        &mut self,
        store: Backend<'a>,
        // threading_strategy: ThreadingStrategy, TODO: implement this
    ) -> StorageResult<&mut Self> {
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
        let dictionary = self.layout.retrieve_attributes(&arr)?;
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
