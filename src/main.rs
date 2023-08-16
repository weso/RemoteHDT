use std::path::PathBuf;
use std::str::FromStr;

use zarr3::codecs::bb::gzip_codec::GzipCodec;
use zarr3::prelude::smallvec::smallvec;
use zarr3::prelude::{create_root_group, ArrayMetadataBuilder, ArrayRegion, GroupMetadata};
use zarr3::store::filesystem::FileSystemStore;
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
        Err(_) => {
            return Err(String::from(
                "Error creating the Group when including the Metadata",
            ))
        }
    };

    let arr_meta = ArrayMetadataBuilder::<i32>::new(&[20, 10])
        .chunk_grid(vec![10, 5].as_slice())
        .unwrap()
        .fill_value(-1)
        .push_bb_codec(GzipCodec::default())
        .build();

    let arr = root_group
        .create_array::<i32>("my_array".parse().unwrap(), arr_meta)
        .unwrap();

    let data = ArcArrayD::from_shape_vec(vec![10, 6], (10..70).collect()).unwrap();

    let offset = smallvec![5, 2];
    arr.write_region(&offset, data).unwrap();

    let output = arr
        .read_region(ArrayRegion::from_offset_shape(&[0, 0], &[20, 10]))
        .unwrap()
        .unwrap();

    println!("{:?}", output);

    Ok(())
}
