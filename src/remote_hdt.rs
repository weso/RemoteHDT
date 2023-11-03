use nalgebra_sparse::CooMatrix;
use nalgebra_sparse::CsrMatrix;
use rayon::prelude::IntoParallelRefIterator;
use rayon::prelude::ParallelBridge;
use rayon::prelude::ParallelIterator;
use rdf_rs::RdfParser;
use serde_json::Map;
use sophia::graph::inmem::sync::FastGraph;
use sophia::graph::GTripleSource;
use sophia::graph::Graph;
use sophia::triple::Triple;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use zarrs::array::Array;
use zarrs::array::ArrayBuilder;
use zarrs::array::DataType;
use zarrs::array::DimensionName;
use zarrs::array::FillValue;
use zarrs::group::GroupBuilder;
use zarrs::storage::store::FilesystemStore;
use zarrs::storage::store::HTTPStore;
use zarrs::storage::ReadableStorageTraits;

use crate::dictionary::Dictionary;
use crate::error::RemoteHDTError;

pub type ZarrArray = CsrMatrix<u32>;
pub type RemoteHDTResult<T> = Result<T, RemoteHDTError>;

const ARRAY_NAME: &str = "/group/RemoteHDT";

pub struct RemoteHDT {
    pub dictionary: Dictionary,
}

impl RemoteHDT {
    pub fn new() -> Self {
        RemoteHDT {
            dictionary: Dictionary::default(),
        }
    }

    /// # Errors
    /// Returns [`PathExistsError`] if the provided path already exists; that is,
    /// the user is trying to store the RDF dataset in an occupied storage. This
    /// is due to the fact that the user may incur in an undefined state.
    pub fn serialize<'a>(
        &mut self,
        zarr_path: &'a str,
        rdf_path: &'a str,
    ) -> RemoteHDTResult<&Self> {
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
        let graph: FastGraph = RdfParser::new(rdf_path).unwrap().graph;
        self.dictionary =
            Dictionary::from_set_terms(graph.subjects()?, graph.predicates()?, graph.objects()?);

        // 4. Build the structure of the Array; as such, several parameters of it are
        // tweaked. Namely, the size of the array, the size of the chunks, the name
        // of the different dimensions and the default values
        let array = ArrayBuilder::new(
            vec![
                self.dictionary.subjects_size() as u64,
                self.dictionary.objects_size() as u64,
            ],
            DataType::UInt32, // TODO: Could this be changed to U8 or to U16?
            vec![1, self.dictionary.objects_size() as u64].into(),
            FillValue::from(0u32), // TODO: the fill value is fine, but it can be improved
        )
        .dimension_names(Some(vec![
            DimensionName::new("Subject"),
            DimensionName::new("Object"),
        ]))
        .attributes({
            let mut attributes = Map::new();
            attributes.insert(
                "subjects".to_string(),
                self.dictionary
                    .subjects()
                    .iter()
                    .par_bridge()
                    .map(|(_, term)| std::str::from_utf8(&term).unwrap().to_string())
                    .collect::<Vec<_>>()
                    .into(),
            );
            attributes.insert(
                "predicates".to_string(),
                self.dictionary
                    .predicates()
                    .iter()
                    .par_bridge()
                    .map(|(_, term)| std::str::from_utf8(&term).unwrap().to_string())
                    .collect::<Vec<_>>()
                    .into(),
            );
            attributes.insert(
                "objects".to_string(),
                self.dictionary
                    .objects()
                    .iter()
                    .par_bridge()
                    .map(|(_, term)| std::str::from_utf8(&term).unwrap().to_string())
                    .collect::<Vec<_>>()
                    .into(),
            );
            attributes
        })
        .build(store, ARRAY_NAME)?;

        array.store_metadata()?;

        // 6. We insert some data into the Array provided a certain shape. That is,
        // we are trying to create an array of a certain Shape (first vector), with
        // the provided values (second vector). What's more, an offset can be set;
        // that is, we can insert the created array with and X and Y shift. Lastly,
        // the region is written provided the aforementioned data and offset
        graph.subjects()?.par_iter().for_each(|subject| {
            let ans = self.create_array(graph.triples_with_s(subject));
            if let Ok(chunk_elements) = ans {
                let _ = array.store_chunk_elements(
                    &[
                        self.dictionary
                            .get_subject_idx_unchecked(&subject.to_string())
                            as u64,
                        0,
                    ],
                    chunk_elements.as_slice(),
                );
            }
        });

        Ok(self)
    }

    fn create_array(&self, triples: GTripleSource<FastGraph>) -> RemoteHDTResult<Vec<u32>> {
        let slice: Vec<AtomicU32> = vec![0; self.dictionary.objects_size()]
            .par_iter()
            .map(|&n| AtomicU32::new(n))
            .collect();

        triples.for_each(|result_triple| match result_triple {
            Ok(triple) => {
                let pidx = self
                    .dictionary
                    .get_predicate_idx(&triple.p().to_string())
                    .unwrap();
                let oidx = self
                    .dictionary
                    .get_object_idx(&triple.o().to_string())
                    .unwrap();
                slice[oidx].store(pidx as u32, Ordering::Relaxed);
            }
            Err(_) => return,
        });

        Ok(slice
            .par_iter()
            .map(|elem| elem.load(Ordering::Relaxed))
            .collect())
    }

    pub fn load<'a>(&mut self, zarr_path: &'a str) -> RemoteHDTResult<ZarrArray> {
        let store = Arc::new(FilesystemStore::new(zarr_path)?);
        let arr = Array::new(store, ARRAY_NAME)?;
        self.retrieve_data(arr)
    }

    pub fn connect<'a>(&mut self, url: &'a str) -> RemoteHDTResult<ZarrArray> {
        let store = Arc::new(HTTPStore::new(url)?);
        let arr = Array::new(store, ARRAY_NAME)?;
        self.retrieve_data(arr)
    }

    fn retrieve_data<T: ReadableStorageTraits + ?Sized>(
        &mut self,
        arr: Array<T>,
    ) -> RemoteHDTResult<ZarrArray> {
        // 4. We get the attributes so we can obtain some values that we will need
        let attributes = arr.attributes();

        let mut subjects = attributes
            .get("subjects")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|term| term.as_str().unwrap())
            .collect::<Vec<&str>>();
        subjects.sort();

        let mut predicates = attributes
            .get("predicates")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|term| term.as_str().unwrap())
            .collect::<Vec<&str>>();
        predicates.sort();

        let mut objects = attributes
            .get("objects")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|term| term.as_str().unwrap())
            .collect::<Vec<&str>>();
        objects.sort();

        self.dictionary = Dictionary::from_vec_str(subjects, predicates, objects);

        // 5. We read the region from the Array we have just created
        let mut matrix = CooMatrix::<u32>::new(arr.shape()[0] as usize, arr.shape()[1] as usize);
        self.dictionary.subjects().iter().for_each(|(i, _)| {
            arr.retrieve_chunk_elements::<u32>(&[i as u64, 0])
                .unwrap()
                .iter()
                .enumerate()
                .for_each(|(j, &value)| {
                    if value != 0u32 {
                        matrix.push(i, j, value);
                    }
                })
        });

        Ok(CsrMatrix::from(&matrix))
    }
}

// TODO: this could be moved to a utils.rs module
pub fn print(matrix: ZarrArray) {
    if matrix.nrows() > 100 || matrix.ncols() > 100 {
        println!("{:?}", matrix.values());
        return;
    }

    let separator = format!("{}+", "+----".repeat(matrix.ncols()));

    matrix.row_iter().for_each(|row| {
        print!("{}\n|", separator);
        for i in 0..row.ncols() {
            match row.get_entry(i) {
                Some(predicate) => print!(" {:^2} |", predicate.into_value()),
                None => print!("{}", 0),
            }
        }
        println!()
    });

    println!("{}", separator);
}
