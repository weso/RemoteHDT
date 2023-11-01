use nalgebra_sparse::CooMatrix;
use nalgebra_sparse::CsrMatrix;
use rayon::prelude::IntoParallelRefIterator;
use rayon::prelude::ParallelBridge;
use rayon::prelude::ParallelIterator;
use rdf_rs::Graph;
use rdf_rs::RdfParser;
use serde_json::Map;
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
use zarrs::storage::ReadableWritableStorage;

use crate::error::RemoteHDTError;

pub type ZarrArray = CsrMatrix<u8>;
pub type RemoteHDTResult<T> = Result<T, RemoteHDTError>;

const ARRAY_NAME: &str = "/group/RemoteHDT";

pub struct RemoteHDT<'a> {
    rdf_path: &'a str,
    store: ReadableWritableStorage<'static>,
}

pub struct RemoteHDTBuilder<'a> {
    rdf_path: &'a str,
    store: ReadableWritableStorage<'static>,
}

impl<'a> RemoteHDTBuilder<'a> {
    pub fn new(zarr_path: &'a str) -> RemoteHDTResult<Self> {
        // 1. First, we open the File System for us to store the ZARR project
        let path = PathBuf::from_str(zarr_path)?;
        let store = Arc::new(FilesystemStore::new(path)?);

        // Set the minimally required fields of RemoteHDT
        Ok(RemoteHDTBuilder {
            rdf_path: Default::default(),
            store,
        })
    }

    pub fn rdf_path(mut self, rdf_path: &'a str) -> Self {
        // Set the RDF path for it to be serialized
        self.rdf_path = rdf_path;
        self
    }

    pub fn build(self) -> RemoteHDT<'a> {
        RemoteHDT {
            rdf_path: self.rdf_path,
            store: self.store,
        }
    }
}

impl<'a> RemoteHDT<'a> {
    pub fn serialize(self) -> RemoteHDTResult<Self> {
        // Create a group and write metadata to filesystem
        let group = GroupBuilder::new().build(self.store.clone(), "/group")?;
        group.store_metadata()?;

        // 3. Import the RDF dump using `rdf-rs`
        let graph = RdfParser::new(self.rdf_path).unwrap().graph; // TODO: remove unwrap

        // 4. Build the structure of the Array; as such, several parameters of it are
        // tweaked. Namely, the size of the array, the size of the chunks, the name
        // of the different dimensions and the default values
        let array = ArrayBuilder::new(
            vec![graph.subjects().len() as u64, graph.objects().len() as u64].into(),
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
                    .map(|subject| subject.to_owned())
                    .collect::<Vec<_>>()
                    .into(),
            );
            attributes.insert(
                "predicates".to_string(),
                graph
                    .predicates()
                    .par_iter()
                    .map(|predicate| predicate.0.to_owned())
                    .collect::<Vec<_>>()
                    .into(),
            );
            attributes.insert(
                "objects".to_string(),
                graph
                    .objects()
                    .par_iter()
                    .map(|object| object.0.to_owned())
                    .collect::<Vec<_>>()
                    .into(),
            );
            attributes
        })
        .build(self.store.clone(), ARRAY_NAME)?;

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
                if ans.is_ok() {
                    let _ = array.store_chunk_elements(&vec![i as u64, 0], ans.unwrap().as_slice());
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

    pub fn load(&mut self) -> RemoteHDTResult<(ZarrArray, Vec<String>, Vec<String>, Vec<String>)> {
        // 3. We import the Array from the FileSystemStore that we have created
        let arr = Array::new(self.store.clone(), ARRAY_NAME)?;

        // 4. We get the attributes so we can obtain some values that we will need
        let attributes = arr.attributes();

        let subjects = attributes
            .get("subjects")
            .unwrap()
            .as_array()
            .unwrap()
            .par_iter()
            .map(|subject| subject.as_str().unwrap().to_string())
            .collect::<Vec<String>>();

        let predicates = attributes
            .get("predicates")
            .unwrap()
            .as_array()
            .unwrap()
            .par_iter()
            .map(|predicate| predicate.as_str().unwrap().to_string())
            .collect::<Vec<String>>();

        let objects = attributes
            .get("objects")
            .unwrap()
            .as_array()
            .unwrap()
            .par_iter()
            .map(|object| object.as_str().unwrap().to_string())
            .collect::<Vec<String>>();

        // 5. We read the region from the Array we have just created
        let mut matrix = CooMatrix::new(subjects.len(), objects.len());
        (0..subjects.len()).for_each(|i| {
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

        Ok((CsrMatrix::from(&matrix), subjects, predicates, objects))
    }
}
