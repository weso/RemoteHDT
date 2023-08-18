use rdf_rs::ntriples::NTriples;
use rdf_rs::{Backend, Triple};
use std::collections::HashSet;
use std::path::PathBuf;
use std::str::FromStr;
use zarr3::codecs::bb::gzip_codec::GzipCodec;
use zarr3::prelude::smallvec::smallvec;
use zarr3::prelude::{create_root_group, ArrayMetadataBuilder, ArrayRegion, GroupMetadata};
use zarr3::store::filesystem::FileSystemStore;
use zarr3::store::NodeName;
use zarr3::ArcArrayD;

pub mod zarr;

fn main() -> Result<(), String> {
    // 1. First, we open the File System for us to store the ZARR project
    let path = match PathBuf::from_str("root.zarr") {
        // TODO: "root.zarr" should be a parameter
        // Error handling in Rust :D
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
    // TODO: "rdf.nt" should be a parameter
    let dump = match NTriples::load("rdf.nt") {
        Ok(dump) => dump,
        Err(_) => return Err(String::from("Error loading the dump")),
    };

    let mut subjects = HashSet::<String>::new();
    let mut predicates = HashSet::<String>::new();
    let mut objects = HashSet::<String>::new();

    dump.triples.iter().for_each(|triple| {
        subjects.insert(triple.subject.to_string());
        predicates.insert(triple.predicate.to_string());
        objects.insert(triple.object.to_string());
    });

    // 4. Build the structure of the Array; as such, several parameters of it are
    // tweaked. Namely, the size of the array, the size of the chunks and the
    // default values
    // TODO: the shape should be dynamically set in accordance to the dimensions of the SPO triples
    // TODO: the size of the chunks should be dynamically set in accordance to the Shape
    let arr_meta = ArrayMetadataBuilder::<i32>::new(&[
        subjects.len() as u64,
        objects.len() as u64,
        predicates.len() as u64,
    ])
    .push_bb_codec(GzipCodec::default())
    .build();

    // 5. Create the Array provided the name of it
    // TODO: "my_array" should be a parameter
    let node_name = match "my_array".parse::<NodeName>() {
        Ok(node_name) => node_name,
        Err(_) => return Err(String::from("Error parsing the NodeName")),
    };

    let arr = match root_group.create_array::<i32>(node_name, arr_meta) {
        Ok(array) => array,
        Err(_) => return Err(String::from("Error creating the Array")),
    };

    // 6. We insert some data into the Array provided a certain shape. That is,
    // we are trying to create an array of a certain Shape (first vector), with
    // the provided values (second vector). What's more, an offset can be set;
    // that is, we can insert the created array with and X and Y shift. Lastly,
    // the region is written provided the aforementioned data and offset
    let data =
        match ArcArrayD::from_shape_vec(vec![subjects.len(), objects.len(), predicates.len()], {
            let mut v = Vec::<i32>::new();
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
        }) {
            Ok(data) => data,
            Err(_) => return Err(String::from("Error creating the data Array")),
        };

    let offset = smallvec![0, 0, 0];

    if arr.write_region(&offset, data).is_err() {
        return Err(String::from("Error writing to the Array"));
    };

    // =========================================================================

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
    ))
}
