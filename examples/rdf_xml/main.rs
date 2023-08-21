use remote_hdt::remote_hdt::{ReferenceSystem, RemoteHDTBuilder};

pub fn main() {
    RemoteHDTBuilder::new("examples/rdf_xml/rdf.rdf", "root.zarr")
        .array_name("array_name")
        .reference_system(ReferenceSystem::OPS)
        .build()
        .load()
        .unwrap()
}
