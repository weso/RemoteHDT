use nalgebra_sparse::CooMatrix;
use nalgebra_sparse::CsrMatrix;
use rayon::prelude::IndexedParallelIterator;
use rayon::prelude::IntoParallelRefIterator;
use rayon::prelude::ParallelIterator;
use rdf_rs::Graph;
use rdf_rs::RdfParser;
use rdf_rs::SimpleTriple;
use serde_json::Map;
use sophia::api::term::CmpTerm;
use sophia::api::term::Term;
use sophia::term::ArcTerm;
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
            DataType::Float32,
            vec![1, graph.objects().len() as u64].into(),
            FillValue::from(0f32),
        )
        .dimension_names(Some(vec![
            DimensionName::new("Subject"),
            DimensionName::new("Object"),
        ]))
        .attributes({
            let mut attributes = Map::new();
            attributes.insert(
                "subjects".to_string(), // TODO: This is not working properly
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
                    .map(|predicate| predicate.to_owned())
                    .collect::<Vec<_>>()
                    .into(),
            );
            attributes.insert(
                "objects".to_string(),
                graph
                    .objects()
                    .par_iter()
                    .map(|object| object.to_owned())
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
            .subjects()
            .par_iter()
            .enumerate()
            .for_each(|(i, term)| {
                let ans = self.create_array(
                    graph
                        .triples()
                        .par_iter()
                        .filter(|triple| term == &triple.subject)
                        .collect(),
                    &graph,
                );
                if ans.is_ok() {
                    let _ = array.store_chunk_elements(&vec![i as u64, 0], ans.unwrap().as_slice());
                }
            });

        Ok(self)
    }

    fn create_array(&self, triples: Vec<&SimpleTriple>, graph: &Graph) -> RemoteHDTResult<Vec<u8>> {
        let slice: Vec<AtomicU8> = vec![0; graph.objects().len()]
            .par_iter()
            .map(|&n| AtomicU8::new(n))
            .collect();

        triples.par_iter().for_each(|triple| {
            let pidx = graph
                .predicates()
                .par_iter()
                .position_any(|elem| elem == &triple.predicate)
                .unwrap();
            let oidx = graph
                .objects()
                .par_iter()
                .position_any(|elem| elem == &triple.object)
                .unwrap();

            slice[oidx].store(pidx as u8, Ordering::Relaxed);
        });

        Ok(slice
            .par_iter()
            .map(|elem| elem.load(Ordering::Relaxed))
            .collect())
    }

    pub fn load(&mut self) -> RemoteHDTResult<ZarrArray> {
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
            .map(|subject| subject.as_str().unwrap().into_term())
            .collect::<Vec<CmpTerm<ArcTerm>>>();

        let predicates = attributes
            .get("predicates")
            .unwrap()
            .as_array()
            .unwrap()
            .par_iter()
            .map(|predicate| predicate.as_str().unwrap().into_term())
            .collect::<Vec<CmpTerm<ArcTerm>>>();

        let objects = attributes
            .get("objects")
            .unwrap()
            .as_array()
            .unwrap()
            .par_iter()
            .map(|object| object.as_str().unwrap().into_term())
            .collect::<Vec<CmpTerm<ArcTerm>>>();

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

        println!("{:?}", matrix);

        Ok(CsrMatrix::from(&matrix))
    }
}
