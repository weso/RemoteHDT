use std::fs::remove_dir_all;

use remote_hdt::remote_hdt::RemoteHDTBuilder;

#[test]
fn write_read_test() {
    let _ = remove_dir_all("root.zarr");

    let remote_hdt = RemoteHDTBuilder::new("root.zarr")
        .reference_system(remote_hdt::remote_hdt::ReferenceSystem::SPO)
        .rdf_path("resources/rdf.nt")
        .array_name("array_name")
        .build()
        .serialize()
        .unwrap();

    let binding = remote_hdt.get_domain();
    let alan_idx = binding.get_subject("http://example.org/alan");

    println!("IDX: {:?}", alan_idx);

    let actual = RemoteHDTBuilder::new("root.zarr")
        .reference_system(remote_hdt::remote_hdt::ReferenceSystem::SPO)
        .array_name("array_name")
        .build()
        .parse()
        .unwrap()
        .get_array()
        .unwrap();

    println!("{:?}", actual);

    // assert_eq!(actual, expected);

    let _ = remove_dir_all("root.zarr");
}
