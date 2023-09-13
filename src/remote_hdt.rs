use ndarray::{ArcArray, ArcArray1, Array2, Axis, Ix3};
use rdf_rs::RdfParser;
use std::path::PathBuf;
use std::str::FromStr;
use zarr3::prelude::smallvec::smallvec;
use zarr3::prelude::{
    create_root_group, Array, ArrayMetadataBuilder, ArrayRegion, GroupMetadata, ReadableMetadata,
};
use zarr3::store::filesystem::FileSystemStore;
use zarr3::store::{NodeKey, NodeName};
use zarr3::{ArcArrayD, CoordVec};

type ArcArray3 = ArcArray<u8, Ix3>;

#[derive(Default)]
pub struct Domain {
    subjects_size: usize,
    predicates_size: usize,
    objects_size: usize,
}

pub enum DimensionName {
    Subject,
    Predicate,
    Object,
}

#[derive(PartialEq)]
pub enum ReferenceSystem {
    SPO,
    SOP,
    PSO,
    POS,
    OSP,
    OPS,
}

pub enum Term {
    Subject(usize),
    Predicate(usize),
    Object(usize),
}

pub struct RemoteHDT<'a> {
    rdf_path: &'a str,
    zarr_path: &'a str,
    array_name: &'a str,
    reference_system: ReferenceSystem,
    array: Option<ArcArray3>,
}

pub struct RemoteHDTBuilder<'a> {
    rdf_path: &'a str,
    zarr_path: &'a str,
    array_name: &'a str,
    reference_system: ReferenceSystem,
    array: Option<ArcArray3>,
}

pub trait Engine {
    fn get(
        &self,
        array: &Option<ArcArray3>,
        term: Term,
        reference_system: &ReferenceSystem,
    ) -> Result<ArcArray3, String> {
        let arr: ArcArray3 = match array {
            Some(arr) => arr.clone(),
            None => return Err(String::from("The array should have been loaded")),
        };

        let binding = arr.to_owned();
        let shape = binding.shape();

        let flattened: ArcArray1<u8> = match term {
            Term::Subject(term) => match reference_system {
                ReferenceSystem::SPO => self.get_first_term(arr, shape[0], term),
                ReferenceSystem::SOP => self.get_first_term(arr, shape[0], term),
                ReferenceSystem::PSO => self.get_second_term(arr, shape[1], term),
                ReferenceSystem::POS => self.get_third_term(arr, shape[2], term),
                ReferenceSystem::OSP => self.get_second_term(arr, shape[1], term),
                ReferenceSystem::OPS => self.get_third_term(arr, shape[2], term),
            },
            Term::Predicate(term) => match reference_system {
                ReferenceSystem::SPO => self.get_second_term(arr, shape[1], term),
                ReferenceSystem::SOP => self.get_third_term(arr, shape[2], term),
                ReferenceSystem::PSO => self.get_first_term(arr, shape[0], term),
                ReferenceSystem::POS => self.get_first_term(arr, shape[0], term),
                ReferenceSystem::OSP => self.get_third_term(arr, shape[2], term),
                ReferenceSystem::OPS => self.get_second_term(arr, shape[1], term),
            },
            Term::Object(term) => match reference_system {
                ReferenceSystem::SPO => self.get_third_term(arr, shape[2], term),
                ReferenceSystem::SOP => self.get_second_term(arr, shape[1], term),
                ReferenceSystem::PSO => self.get_third_term(arr, shape[2], term),
                ReferenceSystem::POS => self.get_second_term(arr, shape[1], term),
                ReferenceSystem::OSP => self.get_first_term(arr, shape[0], term),
                ReferenceSystem::OPS => self.get_first_term(arr, shape[0], term),
            },
        };

        let shaped = match flattened.into_shape(shape) {
            Ok(shaped) => shaped,
            Err(_) => return Err(String::from("Error converting to the required Shape")),
        };

        match shaped.into_dimensionality::<Ix3>() {
            Ok(ans) => Ok(ans),
            Err(_) => Err(String::from("Error assigning the dimensionality")),
        }
    }

    fn get_first_term(&self, array: ArcArray3, size: usize, term: usize) -> ArcArray1<u8> {
        array
            .axis_iter(Axis(0))
            .enumerate()
            .flat_map(|(i, two_dim_array)| {
                let factor: Array2<u8> = if i == term {
                    Array2::eye(size)
                } else {
                    Array2::zeros((size, size))
                };
                factor.dot(&two_dim_array)
            })
            .collect::<ArcArray1<u8>>()
    }

    fn get_second_term(&self, array: ArcArray3, size: usize, term: usize) -> ArcArray1<u8> {
        let mut factor: Array2<u8> = Array2::zeros((size, size));
        factor[[term, 0]] = 1;

        array
            .axis_iter(Axis(0))
            .flat_map(|two_dim_array| factor.dot(&two_dim_array))
            .collect::<ArcArray1<u8>>()
    }

    fn get_third_term(&self, array: ArcArray3, size: usize, term: usize) -> ArcArray1<u8> {
        let mut factor: Array2<u8> = Array2::zeros((size, size));
        factor[[0, term]] = 1;

        array
            .axis_iter(Axis(0))
            .flat_map(|two_dim_array| two_dim_array.dot(&factor))
            .collect::<ArcArray1<u8>>()
    }

    fn get_subject(&self, subject: usize) -> Result<ArcArray3, String>;
    fn get_predicate(&self, predicate: usize) -> Result<ArcArray3, String>;
    fn get_object(&self, object: usize) -> Result<ArcArray3, String>;
}

impl From<DimensionName> for Option<String> {
    fn from(value: DimensionName) -> Self {
        match value {
            DimensionName::Subject => Some("Subject".to_string()),
            DimensionName::Predicate => Some("Predicate".to_string()),
            DimensionName::Object => Some("Object".to_string()),
        }
    }
}

impl From<&ReferenceSystem> for String {
    fn from(value: &ReferenceSystem) -> Self {
        match value {
            ReferenceSystem::SPO => String::from("spo"),
            ReferenceSystem::SOP => String::from("sop"),
            ReferenceSystem::PSO => String::from("pso"),
            ReferenceSystem::POS => String::from("pos"),
            ReferenceSystem::OSP => String::from("osp"),
            ReferenceSystem::OPS => String::from("ops"),
        }
    }
}

impl From<String> for ReferenceSystem {
    fn from(value: String) -> Self {
        match value.as_str() {
            "spo" => ReferenceSystem::SPO,
            "sop" => ReferenceSystem::SOP,
            "pso" => ReferenceSystem::PSO,
            "pos" => ReferenceSystem::POS,
            "osp" => ReferenceSystem::OSP,
            "ops" => ReferenceSystem::OPS,
            _ => ReferenceSystem::SPO,
        }
    }
}

impl ReferenceSystem {
    fn shape(&self, domain: &Domain) -> [usize; 3] {
        match self {
            ReferenceSystem::SPO => [
                domain.subjects_size,
                domain.predicates_size,
                domain.objects_size,
            ],
            ReferenceSystem::SOP => [
                domain.subjects_size,
                domain.objects_size,
                domain.predicates_size,
            ],
            ReferenceSystem::PSO => [
                domain.predicates_size,
                domain.subjects_size,
                domain.objects_size,
            ],
            ReferenceSystem::POS => [
                domain.predicates_size,
                domain.objects_size,
                domain.subjects_size,
            ],
            ReferenceSystem::OSP => [
                domain.objects_size,
                domain.subjects_size,
                domain.predicates_size,
            ],
            ReferenceSystem::OPS => [
                domain.objects_size,
                domain.predicates_size,
                domain.subjects_size,
            ],
        }
    }

    fn shape_u64(&self, domain: &Domain) -> [u64; 3] {
        let shape = self
            .shape(domain)
            .into_iter()
            .map(|dimension| dimension as u64)
            .collect::<Vec<u64>>();

        [shape[0], shape[1], shape[2]]
    }

    fn dimension_names(&self) -> CoordVec<Option<String>> {
        match self {
            ReferenceSystem::SPO => smallvec![
                DimensionName::Subject.into(),
                DimensionName::Predicate.into(),
                DimensionName::Object.into()
            ],
            ReferenceSystem::SOP => smallvec![
                DimensionName::Subject.into(),
                DimensionName::Object.into(),
                DimensionName::Predicate.into(),
            ],
            ReferenceSystem::PSO => smallvec![
                DimensionName::Predicate.into(),
                DimensionName::Subject.into(),
                DimensionName::Object.into()
            ],
            ReferenceSystem::POS => smallvec![
                DimensionName::Predicate.into(),
                DimensionName::Object.into(),
                DimensionName::Subject.into(),
            ],
            ReferenceSystem::OSP => smallvec![
                DimensionName::Object.into(),
                DimensionName::Subject.into(),
                DimensionName::Predicate.into(),
            ],
            ReferenceSystem::OPS => smallvec![
                DimensionName::Object.into(),
                DimensionName::Predicate.into(),
                DimensionName::Subject.into(),
            ],
        }
    }

    fn convert_to(
        &self,
        other: &ReferenceSystem,
        mut array: ArcArray3,
        domain: &Domain,
    ) -> ArcArray3 {
        if self != other {
            // In case the reference system used for serializing the Array is
            // not the same as the one selected by the user when building this
            // struct, we have to reshape the array so it holds to the user's
            // desired mechanism
            let mut v = Vec::<(usize, usize, usize, u8)>::new();

            for (i, outer) in array.outer_iter().enumerate() {
                for j in 0..outer.shape()[0] {
                    for k in 0..outer.shape()[1] {
                        // We convert the reference system used in the serialization
                        // format into SPO
                        v.push(match self {
                            ReferenceSystem::SPO => (i, j, k, outer[[j, k]]),
                            ReferenceSystem::SOP => (i, k, j, outer[[j, k]]),
                            ReferenceSystem::PSO => (j, i, k, outer[[j, k]]),
                            ReferenceSystem::POS => (j, k, i, outer[[j, k]]),
                            ReferenceSystem::OSP => (k, i, j, outer[[j, k]]),
                            ReferenceSystem::OPS => (k, j, i, outer[[j, k]]),
                        })
                    }
                }
            }

            let mut reshaped_array = ArcArray3::zeros(other.shape(&domain));

            for (s, p, o, value) in v {
                match other {
                    ReferenceSystem::SPO => reshaped_array[[s, p, o]] = value,
                    ReferenceSystem::SOP => reshaped_array[[s, o, p]] = value,
                    ReferenceSystem::PSO => reshaped_array[[p, s, o]] = value,
                    ReferenceSystem::POS => reshaped_array[[p, o, s]] = value,
                    ReferenceSystem::OSP => reshaped_array[[o, s, p]] = value,
                    ReferenceSystem::OPS => reshaped_array[[o, p, s]] = value,
                }
            }

            array = reshaped_array;
        }

        array
    }
}

impl<'a> RemoteHDTBuilder<'a> {
    pub fn new(zarr_path: &'a str) -> Self {
        // Set the minimally required fields of RemoteHDT
        RemoteHDTBuilder {
            rdf_path: Default::default(),
            zarr_path,
            array_name: "array",
            reference_system: ReferenceSystem::SPO,
            array: None,
        }
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
            zarr_path: self.zarr_path,
            array_name: self.array_name,
            reference_system: self.reference_system,
            array: self.array,
        }
    }
}

impl<'a> RemoteHDT<'a> {
    pub fn serialize(self) -> Result<Self, String> {
        // 1. First, we open the File System for us to store the ZARR project
        let path = match PathBuf::from_str(self.zarr_path) {
            Ok(path) => path,
            Err(_) => return Err(String::from("Error opening the Path for the ZARR project")),
        };

        let store = match FileSystemStore::create(path, true) {
            Ok(store) => store,
            Err(_) => return Err(String::from("Error creating the File System Store")),
        };

        // 2. We include the default Metadata to the ZARR project
        let root_group = match create_root_group(&store, GroupMetadata::default()) {
            Ok(root_group) => root_group,
            Err(_) => return Err(String::from("Error creating the Group")),
        };

        // 3. Import the RDF dump using `rdf-rs`
        let dump = RdfParser::new(self.rdf_path)?;
        let (subjects, predicates, objects) = dump.extract();

        let domain = &Domain {
            subjects_size: subjects.len(), // size of the unique values for the Subjects
            predicates_size: predicates.len(), // size of the unique values for the Predicates
            objects_size: objects.len(),   // Size of the unique values for the Objects
        };

        // 4. Build the structure of the Array; as such, several parameters of it are
        // tweaked. Namely, the size of the array, the size of the chunks, the name
        // of the different dimensions and the default values
        // TODO: use more than one big chunk?
        // TODO: set the codec
        let arr_meta = ArrayMetadataBuilder::<u8>::new(&self.reference_system.shape_u64(domain))
            .dimension_names(self.reference_system.dimension_names())
            .unwrap()
            .set_attribute("subjects".to_string(), domain.subjects_size)
            .unwrap()
            .set_attribute("predicates".to_string(), domain.predicates_size)
            .unwrap()
            .set_attribute("objects".to_string(), domain.objects_size)
            .unwrap()
            .set_attribute(
                "reference_system".to_string(),
                String::from(&self.reference_system),
            )
            .unwrap()
            .build();

        // 5. Create the Array provided the name of it
        let node_name = match self.array_name.parse::<NodeName>() {
            Ok(node_name) => node_name,
            Err(_) => return Err(String::from("Error parsing the NodeName")),
        };

        let arr = match root_group.create_array::<u8>(node_name, arr_meta) {
            Ok(array) => array,
            Err(_) => return Err(String::from("Error creating the Array")),
        };

        // 6. We insert some data into the Array provided a certain shape. That is,
        // we are trying to create an array of a certain Shape (first vector), with
        // the provided values (second vector). What's more, an offset can be set;
        // that is, we can insert the created array with and X and Y shift. Lastly,
        // the region is written provided the aforementioned data and offset
        let data = match ArcArrayD::from_shape_vec(self.reference_system.shape(domain).to_vec(), {
            let mut v = Vec::<u8>::new();
            for subject in &subjects {
                for predicate in &predicates {
                    for object in &objects {
                        if dump.graph.contains(&[
                            subject.to_owned(),
                            predicate.to_owned(),
                            object.to_owned(),
                        ]) {
                            v.push(1)
                        } else {
                            v.push(0)
                        }
                    }
                }
            }
            v
        }) {
            Ok(data) => data,
            Err(_) => return Err(String::from("Error creating the data Array")),
        };

        let offset = smallvec![0, 0, 0];

        if arr.write_region(&offset, data).is_err() {
            return Err(String::from("Error writing to the Array"));
        };

        // Ok(())

        // =========================================================================
        // TODO: remove this because it's just for debug purposes
        println!("== Subjects =====================================================");
        subjects
            .iter()
            .enumerate()
            .for_each(|(e, i)| println!("{} --> {}", e, i));

        println!("== Predicates ===================================================");
        predicates
            .iter()
            .enumerate()
            .for_each(|(e, i)| println!("{} --> {}", e, i));

        println!("== Objects ======================================================");
        objects
            .iter()
            .enumerate()
            .for_each(|(e, i)| println!("{} --> {}", e, i));

        println!("== Array ========================================================");
        println!(
            "{:?}",
            arr.read_region(ArrayRegion::from_offset_shape(
                &[0, 0, 0],
                &self.reference_system.shape_u64(domain)
            ))
            .unwrap()
            .unwrap()
        );

        Ok(self)
    }

    pub fn parse(mut self) -> Result<Self, String> {
        // 1. First, we open the File System for us to retrieve the ZARR array
        let path = match PathBuf::from_str(self.zarr_path) {
            Ok(path) => path,
            Err(_) => return Err(String::from("Error opening the Path for the ZARR project")),
        };

        let store = match FileSystemStore::open(path) {
            Ok(store) => store,
            Err(_) => return Err(String::from("Error opening the File System Store")),
        };

        // 2. We create the NodeKey from the ArrayName
        let key = match NodeKey::from_str(self.array_name) {
            Ok(key) => key,
            Err(_) => return Err(String::from("Error creating NodeKey from the ArrayName")),
        };

        // 3. We import the Array from the FileSystemStore that we have created
        let arr: Array<'_, FileSystemStore, u8> = match Array::from_store(&store, key) {
            Ok(arr) => arr,
            Err(_) => return Err(String::from("Error importing Array from store")),
        };

        // 4. We get the attributes so we can obtain some values that we will need
        let attributes = arr.get_attributes();

        let domain = &Domain {
            subjects_size: attributes.get("subjects").unwrap().as_u64().unwrap() as usize,
            predicates_size: attributes.get("predicates").unwrap().as_u64().unwrap() as usize,
            objects_size: attributes.get("objects").unwrap().as_u64().unwrap() as usize,
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
        let shape_u64 = &reference_system.shape_u64(domain);
        let array_region = ArrayRegion::from_offset_shape(&[0, 0, 0], shape_u64);

        let region = match arr.read_region(array_region) {
            Ok(region) => region,
            Err(_) => return Err(String::from("Error loading the array")),
        };

        let array = match region {
            Some(array) => array.into_dimensionality::<Ix3>(),
            None => return Err(String::from("Error loading the array")),
        };

        self.array = match array {
            Ok(ans) => Some(reference_system.convert_to(&self.reference_system, ans, domain)),
            Err(_) => return Err(String::from("Error converting to a 3-dimensional array")),
        };

        Ok(self)
    }
}

impl Engine for RemoteHDT<'_> {
    fn get_subject(&self, index: usize) -> Result<ArcArray3, String> {
        self.get(&self.array, Term::Subject(index), &self.reference_system)
    }

    // TODO: the current implementation works for SELECT *, but what if we SELECT predicate?
    fn get_predicate(&self, index: usize) -> Result<ArcArray3, String> {
        self.get(&self.array, Term::Predicate(index), &self.reference_system)
    }

    fn get_object(&self, index: usize) -> Result<ArcArray3, String> {
        self.get(&self.array, Term::Object(index), &self.reference_system)
    }
}

#[cfg(test)]
mod tests {
    use super::{ArcArray3, Domain, Engine, ReferenceSystem, RemoteHDT};

    // We create a SPO array with the following Shape:
    // [       O1  O2  03
    //    P1 [[ 1,  0,  0],
    //    P2  [ 0,  1,  0]], // S1
    //
    //         O1  O2  03
    //    P1 [[ 1,  1,  0],
    //    P2  [ 0,  0,  0]], // S2
    // ]  (Shape: Subjects = 2, Predicates = 2, Objects = 3)
    fn spo_array() -> (ArcArray3, Domain) {
        (
            ArcArray3::from_shape_vec((2, 2, 3), vec![1, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0]).unwrap(),
            Domain {
                subjects_size: 2,
                predicates_size: 2,
                objects_size: 3,
            },
        )
    }

    #[test]
    fn convert_from_spo_2_pso_test() {
        let (array, domain) = spo_array();
        assert_eq!(
            ReferenceSystem::SPO.convert_to(&ReferenceSystem::PSO, array, &domain), // actual
            ArcArray3::from_shape_vec((2, 2, 3), vec![1, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0]).unwrap() // expected
        )
    }

    #[test]
    fn get_subject_test() {
        assert_eq!(
            RemoteHDT {
                rdf_path: Default::default(),
                zarr_path: Default::default(),
                array_name: Default::default(),
                reference_system: ReferenceSystem::SPO,
                array: Some(spo_array().0),
            }
            .get_subject(0)
            .unwrap(),
            ArcArray3::from_shape_vec((2, 2, 3), vec![1, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0]).unwrap()
        )
    }

    #[test]
    fn get_predicate_test() {
        assert_eq!(
            RemoteHDT {
                rdf_path: Default::default(),
                zarr_path: Default::default(),
                array_name: Default::default(),
                reference_system: ReferenceSystem::SPO,
                array: Some(spo_array().0),
            }
            .get_predicate(0)
            .unwrap(),
            ArcArray3::from_shape_vec((2, 2, 3), vec![1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]).unwrap()
        )
    }

    #[test]
    fn get_object_test() {
        assert_eq!(
            RemoteHDT {
                rdf_path: Default::default(),
                zarr_path: Default::default(),
                array_name: Default::default(),
                reference_system: ReferenceSystem::SPO,
                array: Some(spo_array().0),
            }
            .get_object(0)
            .unwrap(),
            ArcArray3::from_shape_vec((2, 2, 3), vec![1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0]).unwrap()
        )
    }
}
