use rdf_rs::ntriples::NTriples;
use rdf_rs::rdf_xml::RdfXml;
use rdf_rs::turtle::Turtle;
use rdf_rs::{Backend, Triple, RDF};
use std::path::PathBuf;
use std::str::FromStr;
use zarr3::prelude::smallvec::smallvec;
use zarr3::prelude::{create_root_group, ArrayMetadataBuilder, ArrayRegion, GroupMetadata};
use zarr3::store::filesystem::FileSystemStore;
use zarr3::store::NodeName;
use zarr3::ArcArrayD;

pub struct RemoteHDT<'a> {
    rdf_path: &'a str,
    zarr_path: &'a str,
    array_name: &'a str,
}

pub struct RemoteHDTBuilder<'a> {
    rdf_path: &'a str,
    zarr_path: &'a str,
    array_name: &'a str,
}

impl<'a> RemoteHDTBuilder<'a> {
    pub fn new(rdf_path: &'a str, zarr_path: &'a str) -> Self {
        // Set the minimally required fields of RemoteHDT
        RemoteHDTBuilder {
            rdf_path,
            zarr_path,
            array_name: "array",
        }
    }

    pub fn array_name(mut self, array_name: &'a str) -> Self {
        // Set the name of the array, and return the builder by value
        self.array_name = array_name;
        self
    }

    pub fn build(self) -> RemoteHDT<'a> {
        RemoteHDT {
            rdf_path: self.rdf_path,
            zarr_path: self.zarr_path,
            array_name: self.array_name,
        }
    }
}

impl<'a> RemoteHDT<'a> {
    pub fn load(self) -> Result<(), String> {
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
        let dump: RDF = match self.rdf_path.split('.').last() {
            Some("nt") => match NTriples::load(self.rdf_path) {
                Ok(dump) => dump,
                Err(_) => return Err(String::from("Error loading the NTriples dump")),
            },
            Some("ttl") => match Turtle::load(self.rdf_path) {
                Ok(dump) => dump,
                Err(_) => return Err(String::from("Error loading the Turtle dump")),
            },
            Some("rdf") => match RdfXml::load(self.rdf_path) {
                Ok(dump) => dump,
                Err(_) => return Err(String::from("Error loading the RDF/XML dump")),
            },
            _ => return Err(String::from("Not supported format for loading the dump")),
        };

        let (subjects, predicates, objects) = dump.extract();

        // 4. Build the structure of the Array; as such, several parameters of it are
        // tweaked. Namely, the size of the array, the size of the chunks, the name
        // of the different dimensions and the default values
        // TODO: use more than one big chunk?
        // TODO: set the codec
        let arr_meta = ArrayMetadataBuilder::<u8>::new(&[
            subjects.len() as u64,
            predicates.len() as u64,
            objects.len() as u64,
        ])
        .dimension_names(smallvec![
            Some("Subjects".to_string()),
            Some("Predicates".to_string()),
            Some("Objects".to_string())
        ])
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
        let data = match ArcArrayD::from_shape_vec(
            vec![subjects.len(), predicates.len(), objects.len()],
            {
                let mut v = Vec::<u8>::new();
                for subject in &subjects {
                    for predicate in &predicates {
                        for object in &objects {
                            if dump.triples.contains(&Triple {
                                subject: subject.to_string(),
                                predicate: predicate.to_string(),
                                object: object.to_string(),
                            }) {
                                v.push(1)
                            } else {
                                v.push(0)
                            }
                        }
                    }
                }
                v
            },
        ) {
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
                &[
                    subjects.len() as u64,
                    predicates.len() as u64,
                    objects.len() as u64
                ]
            ))
            .unwrap()
            .unwrap()
        ))
    }
}
