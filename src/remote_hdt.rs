use ndarray::parallel::prelude::IntoParallelRefIterator;
use ndarray::parallel::prelude::ParallelIterator;
use ndarray::IxDyn;
use ndarray::{ArcArray, Ix3};
use rayon::prelude::IndexedParallelIterator;
use rdf_rs::Graph;
use rdf_rs::RdfParser;
use rdf_rs::SimpleTriple;
use serde_json::Map;
use sophia::api::term::CmpTerm;
use sophia::api::term::Term;
use sophia::term::ArcTerm;
use std::error::Error;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use zarrs::array::Array;
use zarrs::array::ArrayBuilder;
use zarrs::array::DataType;
use zarrs::array::DimensionName;
use zarrs::array::FillValue;
use zarrs::array_subset::ArraySubset;
use zarrs::storage::store::FilesystemStore;
use zarrs::storage::ReadableWritableStorage;
use zarrs::storage::ReadableWritableStorageTraits;

pub type ZarrArray = ArcArray<u8, Ix3>;

pub struct RemoteHDT<'a> {
    rdf_path: &'a str,
    store: ReadableWritableStorage<'static>,
    array_name: &'a str,
}

pub struct RemoteHDTBuilder<'a> {
    rdf_path: &'a str,
    store: ReadableWritableStorage<'static>,
    array_name: &'a str,
}

impl<'a> RemoteHDTBuilder<'a> {
    pub fn new(zarr_path: &'a str) -> Result<Self, String> {
        // 1. First, we open the File System for us to store the ZARR project
        let path = match PathBuf::from_str(zarr_path) {
            Ok(path) => path,
            Err(_) => return Err(String::from("Error opening the Path for the ZARR project")),
        };

        let store = match FilesystemStore::new(path) {
            Ok(store) => Arc::new(store),
            Err(_) => return Err(String::from("Error creating the File System Store")),
        };

        // Set the minimally required fields of RemoteHDT
        Ok(RemoteHDTBuilder {
            rdf_path: Default::default(),
            store,
            array_name: "array",
        })
    }

    pub fn rdf_path(mut self, rdf_path: &'a str) -> Self {
        // Set the RDF path for it to be serialized
        self.rdf_path = rdf_path;
        self
    }

    pub fn array_name(mut self, array_name: &'a str) -> Self {
        // Set the name of the array, and return the builder by value
        self.array_name = array_name;
        self
    }

    pub fn build(self) -> RemoteHDT<'a> {
        RemoteHDT {
            rdf_path: self.rdf_path,
            store: self.store,
            array_name: self.array_name,
        }
    }
}

impl<'a> RemoteHDT<'a> {
    pub fn serialize(self) -> Result<Self, Box<dyn Error>> {
        // 3. Import the RDF dump using `rdf-rs`
        let graph = RdfParser::new(self.rdf_path)?.graph;

        // 4. Build the structure of the Array; as such, several parameters of it are
        // tweaked. Namely, the size of the array, the size of the chunks, the name
        // of the different dimensions and the default values
        let array = ArrayBuilder::new(
            vec![graph.subjects().len() as u64, graph.objects().len() as u64].into(),
            DataType::UInt64,
            vec![1, graph.objects().len() as u64].into(),
            FillValue::from(0u64),
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
        .build(self.store.clone(), "/group/array")?; // TODO: improve this?

        array.store_metadata()?;

        // 6. We insert some data into the Array provided a certain shape. That is,
        // we are trying to create an array of a certain Shape (first vector), with
        // the provided values (second vector). What's more, an offset can be set;
        // that is, we can insert the created array with and X and Y shift. Lastly,
        // the region is written provided the aforementioned data and offset
        graph
            .subjects()
            .par_iter() // TODO: Parallelize this
            .enumerate()
            .for_each(|(i, term)| {
                let chunk_index = vec![i as u64, 0];
                let _ = array.store_chunk_elements(
                    &chunk_index,
                    self.create_array(
                        graph
                            .triples()
                            .par_iter()
                            .filter(|triple| term == &triple.subject)
                            .collect(),
                        &graph,
                    )
                    .unwrap() // TODO: remove unwrap
                    .as_slice(),
                );
            });

        Ok(self)
    }

    fn create_array(&self, triples: Vec<&SimpleTriple>, graph: &Graph) -> Result<Vec<u64>, String> {
        let slice: Vec<AtomicU64> = vec![0; graph.objects().len()]
            .par_iter()
            .map(|&n| AtomicU64::new(n))
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

            slice[oidx].store(pidx as u64, Ordering::Relaxed);
        });

        Ok(slice
            .par_iter()
            .map(|elem| elem.load(Ordering::Relaxed))
            .collect())
    }

    pub fn parse(&self) -> Result<Array<dyn ReadableWritableStorageTraits>, Box<dyn Error>> {
        Ok(Array::new(self.store.clone(), "/group/array")?) // TODO: improve this
    }

    pub fn load(&mut self) -> Result<ArcArray<u8, IxDyn>, String> {
        // 3. We import the Array from the FileSystemStore that we have created
        let arr = match self.parse() {
            Ok(arr) => arr,
            Err(_) => return Err(String::from("Error importing Array from store")),
        };

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
        // TODO: restrict ourselves to a certain region, not to the whole dump :(
        match arr.par_retrieve_array_subset_ndarray(&ArraySubset::new_with_shape(vec![
            subjects.len() as u64,
            objects.len() as u64,
        ])) {
            Ok(region) => Ok(region.into()),
            Err(_) => Err(String::from("Error loading the array")),
        }
    }
}
