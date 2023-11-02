use nalgebra_sparse::CooMatrix;
use nalgebra_sparse::CsrMatrix;
use rayon::prelude::IndexedParallelIterator;
use rayon::prelude::IntoParallelRefIterator;
use rayon::prelude::ParallelBridge;
use rayon::prelude::ParallelIterator;
use rdf_rs::Graph;
use rdf_rs::RdfParser;
use serde_json::Map;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicU8;
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

use crate::error::RemoteHDTError;

pub type ZarrArray = CsrMatrix<u8>;
pub type RemoteHDTResult<T> = Result<T, RemoteHDTError>;

const ARRAY_NAME: &str = "/group/RemoteHDT";

pub struct RemoteHDT {
    subjects: HashMap<String, usize>,
    predicates: HashMap<String, usize>,
    objects: HashMap<String, usize>,
}

impl RemoteHDT {
    pub fn new() -> Self {
        RemoteHDT {
            subjects: HashMap::new(),
            predicates: HashMap::new(),
            objects: HashMap::new(),
        }
    }

    pub fn serialize<'a>(&self, zarr_path: &'a str, rdf_path: &'a str) -> RemoteHDTResult<&Self> {
        let path = PathBuf::from_str(zarr_path)?;
        let store = Arc::new(FilesystemStore::new(path)?);

        // Create a group and write metadata to filesystem
        let group = GroupBuilder::new().build(store.clone(), "/group")?;
        group.store_metadata()?;

        // 3. Import the RDF dump using `rdf-rs`
        let graph = RdfParser::new(rdf_path).unwrap().graph; // TODO: remove unwrap

        // 4. Build the structure of the Array; as such, several parameters of it are
        // tweaked. Namely, the size of the array, the size of the chunks, the name
        // of the different dimensions and the default values
        let array = ArrayBuilder::new(
            vec![graph.subjects().len() as u64, graph.objects().len() as u64],
            DataType::UInt8,
            vec![1, graph.objects().len() as u64].into(),
            FillValue::from(0u8),
        )
        .dimension_names(Some(vec![
            DimensionName::new("Subject"),
            DimensionName::new("Object"),
        ]))
        .attributes({
            let mut attributes = Map::new();
            attributes.insert(
                "subjects".to_string(),
                graph
                    .subjects()
                    .par_iter()
                    .enumerate()
                    .map(|(i, subject)| format!("{}-->{}", i, subject))
                    .collect::<Vec<_>>()
                    .into(),
            );
            attributes.insert(
                "predicates".to_string(),
                graph
                    .predicates()
                    .par_iter()
                    .map(|predicate| format!("{}-->{}", predicate.1, predicate.0))
                    .collect::<Vec<_>>()
                    .into(),
            );
            attributes.insert(
                "objects".to_string(),
                graph
                    .objects()
                    .par_iter()
                    .map(|object| format!("{}-->{}", object.1, object.0))
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
        graph
            .triples()
            .iter()
            .enumerate()
            .par_bridge()
            .for_each(|(i, (_, values))| {
                let ans = self.create_array(values, &graph);
                if let Ok(chunk_elements) = ans {
                    let _ = array.store_chunk_elements(&[i as u64, 0], chunk_elements.as_slice());
                }
            });

        Ok(self)
    }

    fn create_array(
        &self,
        triples: &Vec<(String, String)>,
        graph: &Graph,
    ) -> RemoteHDTResult<Vec<u8>> {
        let slice: Vec<AtomicU8> = vec![0; graph.objects().len()]
            .par_iter()
            .map(|&n| AtomicU8::new(n))
            .collect();

        triples.par_iter().for_each(|(predicate, object)| {
            let pidx = graph.predicates().get(predicate).unwrap().to_owned();
            let oidx = graph.objects().get(object).unwrap().to_owned();
            slice[oidx].store(pidx as u8, Ordering::Relaxed);
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

        self.subjects = attributes
            .get("subjects")
            .unwrap()
            .as_array()
            .unwrap()
            .par_iter()
            .map(|subject| {
                let binding = subject.as_str().unwrap().to_string();
                let arr = binding.split("-->").collect::<Vec<_>>();
                (arr[1].to_string(), FromStr::from_str(arr[0]).unwrap())
            })
            .collect::<HashMap<String, usize>>();

        self.predicates = attributes
            .get("predicates")
            .unwrap()
            .as_array()
            .unwrap()
            .par_iter()
            .map(|predicate| {
                let binding = predicate.as_str().unwrap().to_string();
                let arr = binding.split("-->").collect::<Vec<_>>();
                (arr[1].to_string(), FromStr::from_str(arr[0]).unwrap())
            })
            .collect::<HashMap<String, usize>>();

        self.objects = attributes
            .get("objects")
            .unwrap()
            .as_array()
            .unwrap()
            .par_iter()
            .map(|object| {
                let binding = object.as_str().unwrap().to_string();
                let arr = binding.split("-->").collect::<Vec<_>>();
                (arr[1].to_string(), FromStr::from_str(arr[0]).unwrap())
            })
            .collect::<HashMap<String, usize>>();

        // 5. We read the region from the Array we have just created
        let mut matrix = CooMatrix::new(self.subjects.len(), self.objects.len());
        self.subjects.iter().for_each(|(_, &i)| {
            arr.retrieve_chunk(&[i as u64, 0])
                .unwrap()
                .iter()
                .enumerate()
                .for_each(|(j, value)| {
                    if value != &0u8 {
                        matrix.push(i, j, value.to_owned());
                    }
                })
        });

        Ok(CsrMatrix::from(&matrix))
    }

    pub fn get_subject_idx(&self, subject: &str) -> Option<usize> {
        self.subjects.get(subject).copied()
    }

    pub fn get_subject_idx_unchecked(&self, subject: &str) -> usize {
        self.subjects.get(subject).unwrap().to_owned()
    }

    pub fn get_predicate_idx(&self, predicate: &str) -> Option<usize> {
        self.predicates.get(predicate).copied()
    }

    pub fn get_predicate_idx_unchecked(&self, predicate: &str) -> u8 {
        self.predicates.get(predicate).unwrap().to_owned() as u8
    }

    pub fn get_object_idx(&self, object: &str) -> Option<usize> {
        self.objects.get(object).copied()
    }

    pub fn get_object_idx_unchecked(&self, object: &str) -> usize {
        self.objects.get(object).unwrap().to_owned()
    }
}

pub fn print(matrix: ZarrArray) {
    if matrix.nrows() > 100 || matrix.ncols() > 100 {
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
