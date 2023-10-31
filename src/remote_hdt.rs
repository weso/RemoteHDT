use ndarray::parallel::prelude::IndexedParallelIterator;
use ndarray::parallel::prelude::IntoParallelRefIterator;
use ndarray::parallel::prelude::ParallelIterator;
use ndarray::IxDyn;
use ndarray::{ArcArray, Ix3};
use rdf_rs::RdfParser;
use serde_json::Map;
use sophia::api::prelude::Graph;
use sophia::api::term::CmpTerm;
use sophia::api::term::SimpleTerm;
use sophia::api::term::Term;
use sophia::api::triple::Triple;
use sophia::inmem::index::TermIndexFullError;
use sophia::term::ArcTerm;
use std::error::Error;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use zarrs::array::Array;
use zarrs::array::ArrayBuilder;
use zarrs::array::DataType;
use zarrs::array::FillValue;
use zarrs::array_subset::ArraySubset;
use zarrs::storage::store::FilesystemStore;
use zarrs::storage::ReadableWritableStorage;
use zarrs::storage::ReadableWritableStorageTraits;

use crate::reference_system::ReferenceSystem;

pub type ZarrArray = ArcArray<u8, Ix3>;

#[derive(Default)]
pub struct Domain {
    pub(crate) subjects: Vec<CmpTerm<ArcTerm>>,
    pub(crate) predicates: Vec<CmpTerm<ArcTerm>>,
    pub(crate) objects: Vec<CmpTerm<ArcTerm>>,
}

pub enum Field {
    Subject(usize),
    Predicate(usize),
    Object(usize),
}

pub struct RemoteHDT<'a> {
    rdf_path: &'a str,
    store: ReadableWritableStorage<'static>,
    array_name: &'a str,
    reference_system: ReferenceSystem,
    domain: Domain,
}

pub struct RemoteHDTBuilder<'a> {
    rdf_path: &'a str,
    store: ReadableWritableStorage<'static>,
    array_name: &'a str,
    reference_system: ReferenceSystem,
    domain: Domain,
}

impl Domain {
    pub fn get_subject(&self, subject: &str) -> Option<usize> {
        self.subjects
            .par_iter()
            .position_first(|e| e.to_owned() == subject.into_term::<CmpTerm<ArcTerm>>())
    }

    pub fn get_predicate(&self, predicate: &str) -> Option<usize> {
        self.subjects
            .par_iter()
            .position_first(|e| e.to_owned() == predicate.into_term::<CmpTerm<ArcTerm>>())
    }

    pub fn get_object(&self, object: &str) -> Option<usize> {
        self.subjects
            .par_iter()
            .position_first(|e| e.to_owned() == object.into_term::<CmpTerm<ArcTerm>>())
    }
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
            reference_system: ReferenceSystem::SPO,
            domain: Default::default(),
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

    pub fn reference_system(mut self, reference_system: ReferenceSystem) -> Self {
        // Set the system of reference, and return the builder by value
        self.reference_system = reference_system;
        self
    }

    pub fn build(self) -> RemoteHDT<'a> {
        RemoteHDT {
            rdf_path: self.rdf_path,
            store: self.store,
            array_name: self.array_name,
            reference_system: self.reference_system,
            domain: self.domain,
        }
    }
}

impl<'a> RemoteHDT<'a> {
    pub fn serialize(mut self) -> Result<Self, Box<dyn Error>> {
        // 2. We include the default Metadata to the ZARR project
        // let root_group = GroupBuilder::new().build(self.store, "/group")?;

        // 3. Import the RDF dump using `rdf-rs`
        let dump = RdfParser::new(self.rdf_path)?;

        // 4. Build the structure of the Array; as such, several parameters of it are
        // tweaked. Namely, the size of the array, the size of the chunks, the name
        // of the different dimensions and the default values
        let array = ArrayBuilder::new(
            self.reference_system.shape_u64(&self.domain).to_vec(),
            DataType::UInt8,
            self.reference_system
                .chunk_size_u64(&self.domain)
                .to_vec()
                .into(),
            FillValue::from(0u8),
        )
        .dimension_names(Some(self.reference_system.dimension_names()))
        .attributes({
            let mut attributes = Map::new();
            attributes.insert(
                "subjects".to_string(), // TODO: This is not working properly
                dump.graph
                    .subjects()
                    .map(|subject| format!("{:?}", &subject.unwrap()))
                    .collect::<Vec<_>>()
                    .into(),
            );
            attributes.insert(
                "predicates".to_string(),
                dump.graph
                    .predicates()
                    .map(|predicate| format!("{:?}", &predicate.unwrap()))
                    .collect::<Vec<_>>()
                    .into(),
            );
            attributes.insert(
                "objects".to_string(),
                dump.graph
                    .objects()
                    .map(|object| format!("{:?}", &object.unwrap()))
                    .collect::<Vec<_>>()
                    .into(),
            );
            attributes.insert(
                "reference_system".to_string(),
                String::from(&self.reference_system).into(),
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
        match self.reference_system {
            ReferenceSystem::SPO | ReferenceSystem::SOP => dump.graph.subjects(),
            ReferenceSystem::PSO | ReferenceSystem::POS => dump.graph.predicates(),
            ReferenceSystem::OSP | ReferenceSystem::OPS => dump.graph.objects(),
        } // TODO: Parallelize this
        .enumerate()
        .for_each(|(i, term)| {
            let chunk_index = vec![i as u64, 0, 0];
            let _ = array.store_chunk_elements(
                &chunk_index,
                self.create_array(
                    dump.graph
                        .triples()
                        .filter(|triple| {
                            term.unwrap()
                                == match self.reference_system {
                                    ReferenceSystem::SPO | ReferenceSystem::SOP => {
                                        triple.unwrap().s()
                                    }
                                    ReferenceSystem::PSO | ReferenceSystem::POS => {
                                        triple.unwrap().p()
                                    }
                                    ReferenceSystem::OSP | ReferenceSystem::OPS => {
                                        triple.unwrap().o()
                                    }
                                }
                        })
                        .collect::<Vec<Result<[&SimpleTerm<'static>; 3], TermIndexFullError>>>(),
                    i,
                )
                .unwrap() // TODO: remove unwrap
                .as_slice(),
            );
        });

        Ok(self)
    }

    fn create_array(
        &self,
        triples: Vec<Result<[&SimpleTerm<'static>; 3], TermIndexFullError>>,
        idx: usize,
    ) -> Result<Vec<u8>, String> {
        let slice: Vec<AtomicU8> = vec![0u8; self.reference_system.vec_size(&self.domain)]
            .par_iter()
            .map(|&n| AtomicU8::new(n))
            .collect();

        triples.par_iter().for_each(|triple| {
            let sidx = match self.reference_system {
                ReferenceSystem::SPO | ReferenceSystem::SOP => idx,
                _ => match self
                    .domain
                    .subjects
                    .par_iter()
                    .position_first(|elem| elem == triple.unwrap().s())
                {
                    Some(sidx) => sidx,
                    None => return,
                },
            };
            let pidx = match self.reference_system {
                ReferenceSystem::PSO | ReferenceSystem::POS => idx,
                _ => match self
                    .domain
                    .predicates
                    .par_iter()
                    .position_first(|elem| elem == triple.unwrap().p())
                {
                    Some(pidx) => pidx,
                    None => return,
                },
            };
            let oidx = match self.reference_system {
                ReferenceSystem::OSP | ReferenceSystem::OPS => idx,
                _ => match self
                    .domain
                    .objects
                    .par_iter()
                    .position_first(|elem| elem == triple.unwrap().o())
                {
                    Some(oidx) => oidx,
                    None => return,
                },
            };

            slice[self.reference_system.index(sidx, pidx, oidx, &self.domain)]
                .store(1u8, Ordering::Relaxed);
        });

        Ok(slice
            .par_iter()
            .map(|elem| elem.load(Ordering::Relaxed))
            .collect::<Vec<u8>>())
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

        self.domain = Domain {
            subjects: attributes
                .get("subjects")
                .unwrap()
                .as_array()
                .unwrap()
                .par_iter()
                .map(|subject| subject.as_str().unwrap().into_term())
                .collect::<Vec<CmpTerm<ArcTerm>>>(),
            predicates: attributes
                .get("predicates")
                .unwrap()
                .as_array()
                .unwrap()
                .par_iter()
                .map(|predicate| predicate.as_str().unwrap().into_term())
                .collect::<Vec<CmpTerm<ArcTerm>>>(),
            objects: attributes
                .get("objects")
                .unwrap()
                .as_array()
                .unwrap()
                .par_iter()
                .map(|object| object.as_str().unwrap().into_term())
                .collect::<Vec<CmpTerm<ArcTerm>>>(),
        };

        let reference_system = ReferenceSystem::from(
            attributes
                .get("reference_system")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string(),
        );

        // 5. We read the region from the Array we have just created
        // TODO: restrict ourselves to a certain region, not to the whole dump :(
        match arr.par_retrieve_array_subset_ndarray(&ArraySubset::new_with_shape(
            reference_system.shape_u64(&self.domain).to_vec(),
        )) {
            Ok(region) => Ok(region.into()),
            Err(_) => Err(String::from("Error loading the array")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Domain, ReferenceSystem, ZarrArray};
    use sophia::{
        api::term::{CmpTerm, Term},
        term::ArcTerm,
    };

    // We create a SPO array with the following Shape:
    // [       O1  O2  03
    //    P1 [[ 1,  0,  0],
    //    P2  [ 0,  1,  0]], // S1
    //
    //         O1  O2  03
    //    P1 [[ 1,  1,  0],
    //    P2  [ 0,  0,  0]], // S2
    // ]  (Shape: Subjects = 2, Predicates = 2, Objects = 3)
    fn spo_array<'a>() -> (ZarrArray, Domain) {
        (
            ZarrArray::from_shape_vec((2, 2, 3), vec![1, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0]).unwrap(),
            Domain {
                subjects: vec![
                    "S1".into_term::<CmpTerm<ArcTerm>>(),
                    "S2".into_term::<CmpTerm<ArcTerm>>(),
                ],
                predicates: vec![
                    "P1".into_term::<CmpTerm<ArcTerm>>(),
                    "P2".into_term::<CmpTerm<ArcTerm>>(),
                ],
                objects: vec![
                    "O1".into_term::<CmpTerm<ArcTerm>>(),
                    "O2".into_term::<CmpTerm<ArcTerm>>(),
                    "O3".into_term::<CmpTerm<ArcTerm>>(),
                ],
            },
        )
    }

    #[test]
    fn convert_from_spo_2_pso_test() {
        let (array, domain) = spo_array();
        assert_eq!(
            ReferenceSystem::SPO.convert_to(&ReferenceSystem::PSO, array, &domain), // actual
            ZarrArray::from_shape_vec((2, 2, 3), vec![1, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0]).unwrap() // expected
        )
    }

    // #[test] TODO: fix this
    // fn get_subject_test() {
    //     assert_eq!(
    //         RemoteHDT {
    //             rdf_path: Default::default(),
    //             zarr_path: Default::default(),
    //             array_name: Default::default(),
    //             reference_system: ReferenceSystem::SPO,
    //             array: Some(spo_array().0),
    //             domain: spo_array().1,
    //             engine: ArrayEngine,
    //         }
    //         .get_subject(0)
    //         .unwrap(),
    //         ArcArrayD::from_shape_vec((2, 2, 3), vec![1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]).unwrap()
    //     )
    // }

    // #[test]
    // fn get_predicate_test() {
    //     assert_eq!(
    //         RemoteHDT {
    //             rdf_path: Default::default(),
    //             zarr_path: Default::default(),
    //             array_name: Default::default(),
    //             reference_system: ReferenceSystem::SPO,
    //             array: Some(spo_array().0),
    //             domain: spo_array().1,
    //             engine: ArrayEngine,
    //         }
    //         .get_predicate(0)
    //         .unwrap(),
    //         ArcArrayD::from_shape_vec((2, 2, 3), vec![1, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0]).unwrap()
    //     )
    // }

    // #[test]
    // fn get_object_test() {
    //     assert_eq!(
    //         RemoteHDT {
    //             rdf_path: Default::default(),
    //             zarr_path: Default::default(),
    //             array_name: Default::default(),
    //             reference_system: ReferenceSystem::SPO,
    //             array: Some(spo_array().0),
    //             domain: spo_array().1,
    //         }
    //         .get_object(0)
    //         .unwrap(),
    //         ArcArrayD::from_shape_vec((2, 2, 3), vec![1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0]).unwrap()
    //     )
    // }
}
