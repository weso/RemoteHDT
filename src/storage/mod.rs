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

use crate::dictionary::Dictionary;
use crate::error::RemoteHDTError;
use crate::io::RdfParser;
use crate::utils::term_to_value;

use self::layout::Layout;

mod layout;
pub mod matrix;
pub mod tabular;

pub type ZarrArray = CsMat<u8>;
pub type StorageResult<T> = Result<T, RemoteHDTError>;
pub type LocalStorage<T> = Storage<FilesystemStore, T>;
pub type HTTPStorage<T> = Storage<HTTPStore, T>;

const ARRAY_NAME: &str = "/group/RemoteHDT";

pub enum ChunkingStrategy {
    Chunk,
    Sharding(u64),
}

impl From<ChunkingStrategy> for u64 {
    fn from(value: ChunkingStrategy) -> Self {
        match value {
            ChunkingStrategy::Chunk => 1,
            ChunkingStrategy::Sharding(size) => size,
        }
    }
}

pub struct Storage<R, T> {
    dictionary: Dictionary,
    layout: Box<dyn Layout<R, T>>,
}

impl<R, T: TriviallyTransmutable> Storage<R, T> {
    pub fn new(layout: impl Layout<R, T> + 'static) -> Self {
        Storage {
            dictionary: Default::default(),
            layout: Box::new(layout),
        }
    }

    pub fn get_dictionary(&self) -> Dictionary {
        self.dictionary.to_owned()
    }
}

impl<T: TriviallyTransmutable> LocalStorage<T> {
    /// # Errors
    /// Returns [`PathExistsError`] if the provided path already exists; that is,
    /// the user is trying to store the RDF dataset in an occupied storage. This
    /// is due to the fact that the user may incur in an undefined state.
    pub fn serialize<'a>(
        &mut self,
        zarr_path: &'a str,
        rdf_path: &'a str,
        chunking_strategy: ChunkingStrategy,
    ) -> StorageResult<&Self> {
        // 1. The first thing that should be done is to check whether the path
        // in which we are trying to store the dump already exists or not. If it
        // does, we should stop the execution, preventing the user from losing
        // data. Otherwise we can resume it and begin the actual proccess...
        let path = PathBuf::from_str(zarr_path)?;
        if path.exists() {
            // the actual check occurs here !!!
            return Err(RemoteHDTError::PathExistsError);
        }

        // 2. We can create the FileSystemStore appropiately
        let store = Arc::new(FilesystemStore::new(path)?);

        // Create a group and write metadata to filesystem
        let group = GroupBuilder::new().build(store.clone(), "/group")?;
        group.store_metadata()?;

        // 3. Import the RDF dump using `rdf-rs`
        let (graph, dictionary) = RdfParser::new(rdf_path).unwrap().parse().unwrap(); // TODO: remove unwraps
        self.dictionary = dictionary;

        // 4. Build the structure of the Array; as such, several parameters of it are
        // tweaked. Namely, the size of the array, the size of the chunks, the name
        // of the different dimensions and the default values
        let arr = ArrayBuilder::new(
            self.layout.shape(&self.dictionary, &graph),
            self.layout.data_type(),
            self.layout.chunk_shape(chunking_strategy, &self.dictionary),
            self.layout.fill_value(),
        )
        .dimension_names(self.layout.dimension_names())
        .array_to_bytes_codec(self.layout.array_to_bytes_codec(&self.dictionary)?)
        .attributes({
            let mut attributes = Map::new();
            attributes.insert("subjects".into(), term_to_value(self.dictionary.subjects()));
            attributes.insert(
                "predicates".into(),
                term_to_value(self.dictionary.predicates()),
            );
            attributes.insert("objects".into(), term_to_value(self.dictionary.objects()));
            attributes
        }) // TODO: one attribute should be the Layout
        .build(store, ARRAY_NAME)?;

        arr.store_metadata()?;

        self.layout.serialize(arr, &self.dictionary, graph)?;

        Ok(self)
    }

    pub fn load(&mut self, zarr_path: &str) -> StorageResult<Array<FilesystemStore>> {
        let store = Arc::new(FilesystemStore::new(zarr_path)?);
        let arr = Array::new(store, ARRAY_NAME)?;
        self.dictionary = self.layout.retrieve_attributes(&arr);
        Ok(arr)
    }

    // TODO: improve this naming convention
    pub fn load_sparse(&mut self, zarr_path: &str) -> StorageResult<ZarrArray> {
        let arr = self.load(zarr_path)?;
        self.layout.parse(arr, &self.dictionary)
    }
}

impl<T: TriviallyTransmutable> HTTPStorage<T> {
    pub fn connect(&mut self, url: &str) -> StorageResult<Array<HTTPStore>> {
        let store = Arc::new(HTTPStore::new(url)?);
        let arr = Array::new(store, ARRAY_NAME)?;
        self.dictionary = self.layout.retrieve_attributes(&arr);
        Ok(arr)
    }

    // TODO: improve this naming convention
    pub fn connect_sparse(&mut self, url: &str) -> StorageResult<ZarrArray> {
        let arr = self.connect(url)?;
        self.layout.parse(arr, &self.dictionary)
    }
}
