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

pub enum ReferenceSystem {
    SPO,
    SOP,
    PSO,
    POS,
    OSP,
    OPS,
}

pub struct RemoteHDT<'a> {
    rdf_path: &'a str,
    zarr_path: &'a str,
    array_name: &'a str,
    reference_system: ReferenceSystem,
}

pub struct RemoteHDTBuilder<'a> {
    rdf_path: &'a str,
    zarr_path: &'a str,
    array_name: &'a str,
    reference_system: ReferenceSystem,
}

pub trait Engine {
    fn get_predicate(dump: RemoteHDT, index: u32);
    fn get_object(dump: RemoteHDT, index: u32);
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
}

impl<'a> RemoteHDTBuilder<'a> {
    pub fn new(zarr_path: &'a str) -> Self {
        // Set the minimally required fields of RemoteHDT
        RemoteHDTBuilder {
            rdf_path: Default::default(),
            zarr_path,
            array_name: "array",
            reference_system: ReferenceSystem::SPO,
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
        }
    }
}

impl<'a> RemoteHDT<'a> {
    pub fn from_rdf(self) -> Result<(), String> {
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

        // =========================================================================

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
        Ok(println!(
            "{:?}",
            arr.read_region(ArrayRegion::from_offset_shape(
                &[0, 0, 0],
                &self.reference_system.shape_u64(domain)
            ))
            .unwrap()
            .unwrap()
        ))
    }

    pub fn into_ndarray(self) -> Result<ArcArrayD<u8>, String> {
        // 1. First, we open the File System for us to retrieve the ZARR array
        let path = match PathBuf::from_str(self.zarr_path) {
            Ok(path) => path,
            Err(_) => return Err(String::from("Error opening the Path for the ZARR project")),
        };

        let store = match FileSystemStore::open(path) {
            Ok(store) => store,
            Err(_) => return Err(String::from("Error opening the File System Store")),
        };

        // 2. We include the default Metadata to the ZARR project
        // TODO: remove unwraps and use match :D
        let key = NodeKey::from_str(self.array_name).unwrap();
        let arr: Array<'_, FileSystemStore, u8> = Array::from_store(&store, key).unwrap();

        let attributes = arr.get_attributes();

        let domain = &Domain {
            subjects_size: attributes.get("subjects").unwrap().as_u64().unwrap() as usize,
            predicates_size: attributes.get("predicates").unwrap().as_u64().unwrap() as usize,
            objects_size: attributes.get("objects").unwrap().as_u64().unwrap() as usize,
        };

        let reference_system: ReferenceSystem = ReferenceSystem::from(
            attributes
                .get("reference_system")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string(),
        );

        Ok(arr
            .read_region(ArrayRegion::from_offset_shape(
                &[0, 0, 0],
                &reference_system.shape_u64(domain),
            ))
            .unwrap()
            .unwrap())
    }
}
