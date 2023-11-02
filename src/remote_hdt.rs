use nalgebra_sparse::CooMatrix;
use nalgebra_sparse::CsrMatrix;
use rayon::prelude::IntoParallelRefIterator;
use rayon::prelude::ParallelIterator;
use rdf_rs::RdfParser;
use serde_json::Map;
use sophia::graph::indexed::IndexedGraph;
use sophia::graph::inmem::sync::FastGraph;
use sophia::graph::GTripleSource;
use sophia::graph::Graph;
use sophia::triple::Triple;
use std::collections::HashMap;
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

use crate::error::RemoteHDTError;

pub type ZarrArray = CsrMatrix<u32>;
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
        if path.exists() {
            return Err(RemoteHDTError::PathExistsError);
        }

        let store = Arc::new(FilesystemStore::new(path)?);

        // Create a group and write metadata to filesystem
        let group = GroupBuilder::new().build(store.clone(), "/group")?;
        group.store_metadata()?;

        // 3. Import the RDF dump using `rdf-rs`
        let graph: FastGraph = RdfParser::new(rdf_path).unwrap().graph;

        // TODO: This should be improved, currently, the size of the array will
        // be computed in a not-so efficient manner. That system will pick the
        // highest index and choose that as the size of the rows and the columns
        let max_subject = graph
            .subjects()?
            .par_iter()
            .map(|term| graph.get_index(term).unwrap())
            .max()
            .unwrap();

        let max_predicate = graph
            .predicates()?
            .par_iter()
            .map(|term| graph.get_index(term).unwrap())
            .max()
            .unwrap();

        let max_object = graph
            .objects()?
            .par_iter()
            .map(|term| graph.get_index(term).unwrap())
            .max()
            .unwrap();

        let size = *[max_subject, max_predicate, max_object]
            .iter()
            .max()
            .unwrap() as u64
            + 1;

        // 4. Build the structure of the Array; as such, several parameters of it are
        // tweaked. Namely, the size of the array, the size of the chunks, the name
        // of the different dimensions and the default values
        let array = ArrayBuilder::new(
            vec![size, size],      // TODO: this should be vec![subjects.len(), objects.len()]
            DataType::UInt32,      // TODO: Could this be changed to U8 or to U16?
            vec![1, size].into(), // TODO: the size of the chunk should be [1, objects.len()]; that is, 1 row
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
                graph // TODO: we are doing the same process three times --> Function
                    .subjects()?
                    .par_iter()
                    .map(|term| {
                        // TODO: this process could be improved by using our own Indices
                        format!("{}-->{}", term.to_string(), graph.get_index(term).unwrap())
                    })
                    .collect::<Vec<_>>()
                    .into(),
            );
            attributes.insert(
                "predicates".to_string(),
                graph // TODO: we are doing the same process three times --> Function
                    .predicates()?
                    .par_iter()
                    .map(|term| {
                        // TODO: this process could be improved by using our own Indices
                        format!("{}-->{}", term.to_string(), graph.get_index(term).unwrap())
                    })
                    .collect::<Vec<_>>()
                    .into(),
            );
            attributes.insert(
                "objects".to_string(),
                graph // TODO: we are doing the same process three times --> Function
                    .objects()?
                    .par_iter()
                    .map(|term| {
                        // TODO: this process could be improved by using our own Indices
                        format!("{}-->{}", term.to_string(), graph.get_index(term).unwrap())
                    })
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
            let i = graph.get_index(subject).unwrap();
            let ans = self.create_array(graph.triples_with_s(subject), &graph, size);
            if let Ok(chunk_elements) = ans {
                let _ = array.store_chunk_elements(&[i as u64, 0], chunk_elements.as_slice());
            }
        });

        Ok(self)
    }

    fn create_array(
        &self,
        triples: GTripleSource<FastGraph>,
        graph: &FastGraph,
        size: u64, // TODO: this is avoidable...
    ) -> RemoteHDTResult<Vec<u32>> {
        let slice: Vec<AtomicU32> = vec![0; size.try_into().unwrap()]
            .par_iter()
            .map(|&n| AtomicU32::new(n))
            .collect();

        triples.for_each(|result_triple| match result_triple {
            Ok(triple) => {
                let pidx = graph.get_index(triple.p()).unwrap() as u32;
                let oidx = graph.get_index(triple.o()).unwrap() as usize;
                slice[oidx].store(pidx, Ordering::Relaxed);
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

        self.subjects = attributes
            .get("subjects")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|subject| {
                let binding = subject.as_str().unwrap().to_string();
                let arr = binding.split("-->").collect::<Vec<_>>();
                (arr[0].to_string(), FromStr::from_str(arr[1]).unwrap())
            })
            .collect::<HashMap<String, usize>>();

        self.predicates = attributes
            .get("predicates")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|predicate| {
                let binding = predicate.as_str().unwrap().to_string();
                let arr = binding.split("-->").collect::<Vec<_>>();
                (arr[0].to_string(), FromStr::from_str(arr[1]).unwrap())
            })
            .collect::<HashMap<String, usize>>();

        self.objects = attributes
            .get("objects")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|object| {
                let binding = object.as_str().unwrap().to_string();
                let arr = binding.split("-->").collect::<Vec<_>>();
                (arr[0].to_string(), FromStr::from_str(arr[1]).unwrap())
            })
            .collect::<HashMap<String, usize>>();

        // 5. We read the region from the Array we have just created
        let mut matrix = CooMatrix::<u32>::new(arr.shape()[0] as usize, arr.shape()[1] as usize);
        self.subjects.iter().for_each(|(_, &i)| {
            arr.retrieve_chunk_elements::<u32>(&[i as u64, 0])
                .unwrap()
                .iter()
                .enumerate()
                .for_each(|(j, value)| {
                    if value != &0u32 {
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

    pub fn get_predicate_idx_unchecked(&self, predicate: &str) -> u32 {
        self.predicates.get(predicate).unwrap().to_owned() as u32
    }

    pub fn get_object_idx(&self, object: &str) -> Option<usize> {
        self.objects.get(object).copied()
    }

    pub fn get_object_idx_unchecked(&self, object: &str) -> usize {
        self.objects.get(object).unwrap().to_owned()
    }
}

// TODO: this could be moved to a utils.rs module
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
