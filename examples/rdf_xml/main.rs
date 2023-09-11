use remote_hdt::remote_hdt::{ReferenceSystem, RemoteHDTBuilder};

pub fn main() {
    RemoteHDTBuilder::new("root.zarr")
        .rdf_path("examples/rdf_xml/rdf.rdf")
        .array_name("array_name")
        .reference_system(ReferenceSystem::OPS)
        .build()
        .from_rdf()
        .unwrap()
}
