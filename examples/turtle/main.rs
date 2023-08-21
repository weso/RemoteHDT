use remote_hdt::remote_hdt::{ReferenceSystem, RemoteHDTBuilder};

pub fn main() {
    RemoteHDTBuilder::new("examples/turtle/rdf.ttl", "root.zarr")
        .array_name("array_name")
        .reference_system(ReferenceSystem::PSO)
        .build()
        .load()
        .unwrap()
}
