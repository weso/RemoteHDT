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
use zarrs::storage::WritableStorageTraits;

use crate::error::RemoteHDTError;
use crate::io::RdfParser;

use self::dictionary::Dictionary;
use self::matrix::MatrixLayout;
use self::private::LayoutOps;
use self::tabular::TabularLayout;
use self::utils::term_to_value;

pub mod dictionary;
pub mod matrix;
pub mod tabular;
mod utils;

pub type ZarrArray = CsMat<u8>;
pub type StorageResult<T> = Result<T, RemoteHDTError>;

const ARRAY_NAME: &str = "/group/RemoteHDT";

mod private {
    use zarrs::array::codec::ArrayToBytesCodecTraits;
    use zarrs::array::codec::BytesToBytesCodecTraits;
    use zarrs::array::Array;
    use zarrs::array::ChunkGrid;
    use zarrs::array::DataType;
    use zarrs::array::DimensionName;
    use zarrs::array::FillValue;
    use zarrs::storage::ReadableStorageTraits;
    use zarrs::storage::WritableStorageTraits;

    use crate::io::Graph;

    use super::dictionary::Dictionary;
    use super::utils::value_to_term;
    use super::StorageResult;
    use super::ZarrArray;

    pub trait LayoutFields {
        fn set_dictionary(&mut self, dictionary: Dictionary);
        fn get_dictionary(&self) -> Dictionary;
        fn set_graph(&mut self, graph: Graph);
        fn get_graph(&self) -> Graph;
        fn set_rdf_path(&mut self, rdf_path: String);
        fn get_rdf_path(&self) -> String;
    }

    pub trait LayoutConfiguration {
        fn shape(&self) -> Vec<u64>;
        fn data_type(&self) -> DataType;
        fn chunk_shape(&self) -> ChunkGrid;
        fn fill_value(&self) -> FillValue;
        fn dimension_names(&self) -> Option<Vec<DimensionName>>;
        fn array_to_bytes_codec(&self) -> StorageResult<Box<dyn ArrayToBytesCodecTraits>>;
        fn bytes_to_bytes_codec(&self) -> Vec<Box<dyn BytesToBytesCodecTraits>>;
    }

    pub trait LayoutOps<R: ReadableStorageTraits + Sized, W: WritableStorageTraits + Sized>:
        LayoutFields + LayoutConfiguration
    {
        fn retrieve_attributes(&mut self, arr: &Array<R>) {
            // 4. We get the attributes so we can obtain some values that we will need
            let attributes = arr.attributes();

            let subjects = &value_to_term(attributes.get("subjects").unwrap());
            let predicates = &value_to_term(attributes.get("predicates").unwrap());
            let objects = &value_to_term(attributes.get("objects").unwrap());

            self.set_dictionary(Dictionary::from_vec_str(subjects, predicates, objects))
        }

        fn into_file(&mut self, arr: Array<W>) -> StorageResult<()>;

        fn from_file(&mut self, arr: Array<R>) -> StorageResult<ZarrArray>;
    }
}

pub enum Layout {
    Tabular,
    Matrix,
}

pub struct Storage<R: ReadableStorageTraits + Sized, W: WritableStorageTraits + Sized> {
    layout: Box<dyn LayoutOps<R, W>>,
}

impl<R: ReadableStorageTraits + Sized, W: WritableStorageTraits + Sized> Storage<R, W> {
    pub fn new(layout: Layout) -> Self {
        Storage {
            layout: match layout {
                Layout::Tabular => Box::new(TabularLayout::default()) as Box<dyn LayoutOps<R, W>>,
                Layout::Matrix => Box::new(MatrixLayout::default()) as Box<dyn LayoutOps<R, W>>,
            },
        }
    }

    pub fn get_dictionary(&self) -> Dictionary {
        self.layout.get_dictionary()
    }
}

impl Storage<FilesystemStore, FilesystemStore> {
    /// # Errors
    /// Returns [`PathExistsError`] if the provided path already exists; that is,
    /// the user is trying to store the RDF dataset in an occupied storage. This
    /// is due to the fact that the user may incur in an undefined state.
    pub fn serialize<'a>(&mut self, zarr_path: &'a str, rdf_path: &'a str) -> StorageResult<&Self> {
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

        self.layout.set_dictionary(dictionary);
        self.layout.set_graph(graph);
        self.layout.set_rdf_path(rdf_path.to_string());

        // 4. Build the structure of the Array; as such, several parameters of it are
        // tweaked. Namely, the size of the array, the size of the chunks, the name
        // of the different dimensions and the default values
        let arr = ArrayBuilder::new(
            self.layout.shape(),
            self.layout.data_type(),
            self.layout.chunk_shape(),
            self.layout.fill_value(),
        )
        .dimension_names(self.layout.dimension_names())
        .bytes_to_bytes_codecs(self.layout.bytes_to_bytes_codec())
        .array_to_bytes_codec(self.layout.array_to_bytes_codec()?)
        .attributes({
            let mut attributes = Map::new();
            attributes.insert(
                "subjects".to_string(),
                term_to_value(self.layout.get_dictionary().subjects()),
            );
            attributes.insert(
                "predicates".to_string(),
                term_to_value(self.layout.get_dictionary().predicates()),
            );
            attributes.insert(
                "objects".to_string(),
                term_to_value(self.layout.get_dictionary().objects()),
            );
            attributes
        })
        .build(store, ARRAY_NAME)?;

        arr.store_metadata()?;

        self.layout.into_file(arr)?;

        Ok(self)
    }

    pub fn load<'a>(&mut self, zarr_path: &'a str) -> StorageResult<Array<FilesystemStore>> {
        let store = Arc::new(FilesystemStore::new(zarr_path)?);
        let arr = Array::new(store, ARRAY_NAME)?;
        self.layout.retrieve_attributes(&arr);
        Ok(arr)
    }

    pub fn load_sparse<'a>(&mut self, zarr_path: &'a str) -> StorageResult<ZarrArray> {
        let arr = self.load(zarr_path)?;
        self.layout.from_file(arr)
    }
}

impl Storage<HTTPStore, FilesystemStore> {
    pub fn connect<'a>(&mut self, url: &'a str) -> StorageResult<Array<HTTPStore>> {
        let store = Arc::new(HTTPStore::new(url)?);
        let arr = Array::new(store, ARRAY_NAME)?;
        self.layout.retrieve_attributes(&arr);
        Ok(arr)
    }

    pub fn connect_sparse<'a>(&mut self, url: &'a str) -> StorageResult<ZarrArray> {
        let arr = self.connect(url)?;
        self.layout.from_file(arr)
    }
}
