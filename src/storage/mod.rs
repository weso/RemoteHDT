use safe_transmute::TriviallyTransmutable;
use serde_json::Map;
use sprs::CsMat;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use zarrs::array::Array;
use zarrs::array::ArrayBuilder;
use zarrs::group::GroupBuilder;
use zarrs::storage::store::FilesystemStore;
use zarrs::storage::store::HTTPStore;
use zarrs::storage::ReadableStorageTraits;

use crate::dictionary::Dictionary;
use crate::error::RemoteHDTError;
use crate::io::Graph;
use crate::io::RdfParser;
use crate::utils::rdf_to_value;

use self::layout::Layout;
use self::params::ChunkingStrategy;
use self::params::Dimensionality;
use self::params::ReferenceSystem;
use self::params::Serialization;

mod layout;
pub mod matrix;
pub mod ops;
pub mod params;
pub mod tabular;

pub type ZarrArray = CsMat<u8>;
pub type StorageResult<T> = Result<T, RemoteHDTError>;
pub type LocalStorage<T, C> = Storage<FilesystemStore, T, C>;
pub type HTTPStorage<T, C> = Storage<HTTPStore, T, C>;

const ARRAY_NAME: &str = "/group/RemoteHDT"; // TODO: parameterize this

pub struct Storage<R, T, C> {
    dictionary: Dictionary,
    dimensionality: Dimensionality,
    layout: Box<dyn Layout<R, T, C>>,
    serialization: Serialization,
    reference_system: ReferenceSystem,
    array: Option<Array<R>>,
    sparse_array: Option<ZarrArray>,
}

impl<R: ReadableStorageTraits, T: TriviallyTransmutable, C> Storage<R, T, C> {
    pub fn new(layout: impl Layout<R, T, C> + 'static, serialization: Serialization) -> Self {
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

    fn process_zarr(&mut self, storage: R) -> StorageResult<&Self> {
        let store = Arc::new(storage);
        let arr = Array::new(store, ARRAY_NAME)?;
        let (dictionary, ref_system) = self.layout.retrieve_attributes(&arr)?;
        self.dictionary = dictionary;
        self.reference_system = ref_system;
        self.dimensionality =
            Dimensionality::new(&self.reference_system, &self.dictionary, &Graph::default());

        match self.serialization {
            Serialization::Zarr => self.array = Some(arr),
            Serialization::Sparse => {
                self.sparse_array = Some(self.layout.parse(&arr, &self.dimensionality)?)
            }
        }

        Ok(self)
    }
}

impl<T: TriviallyTransmutable, C> LocalStorage<T, C> {
    /// # Errors
    /// Returns [`PathExistsError`] if the provided path already exists; that is,
    /// the user is trying to store the RDF dataset in an occupied storage. This
    /// is due to the fact that the user may incur in an undefined state.
    pub fn serialize<'a>(
        &mut self,
        zarr_path: &'a str,
        rdf_path: &'a str,
        chunking_strategy: ChunkingStrategy,
        reference_system: ReferenceSystem,
        // threading_strategy: ThreadingStrategy,
    ) -> StorageResult<&Self> {
        // 1. The first thing that should be done is to check whether the path
        // in which we are trying to store the dump already exists or not. If it
        // does, we should stop the execution, preventing the user from losing
        // data. Otherwise we can resume it and begin the actual proccess...
        let path = PathBuf::from_str(zarr_path)?;
        if path.exists() {
            // the actual check occurs here !!!
            return Err(RemoteHDTError::PathExists);
        }

        // 2. We can create the FileSystemStore appropiately
        let store = Arc::new(FilesystemStore::new(path)?);

        // Create a group and write metadata to filesystem
        let group = GroupBuilder::new().build(store.clone(), "/group")?;
        group.store_metadata()?;

        // TODO: rayon::ThreadPoolBuilder::new()
        //     .num_threads(1)
        //     .build_global()
        //     .unwrap();

        // 3. Import the RDF dump using `rdf-rs`
        let graph = match RdfParser::parse(rdf_path, &reference_system) {
            Ok((graph, dictionary)) => {
                self.dictionary = dictionary;
                self.dimensionality =
                    Dimensionality::new(&reference_system, &self.dictionary, &graph);
                graph
            }
            Err(_) => todo!(),
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
        }) // TODO: one attribute should be the Layout
        .build(store, ARRAY_NAME)?;

        arr.store_metadata()?;

        self.layout.serialize(arr, graph)?;

        Ok(self)
    }

    pub fn load(&mut self, zarr_path: &str) -> StorageResult<&Self> {
        self.process_zarr(FilesystemStore::new(zarr_path)?)
    }
}

impl<T: TriviallyTransmutable, C> HTTPStorage<T, C> {
    pub fn connect(&mut self, url: &str) -> StorageResult<&Self> {
        self.process_zarr(HTTPStore::new(url)?)
    }
}
