use remote_hdt::remote_hdt::{ReferenceSystem, RemoteHDTBuilder};

pub fn main() {
    let _ = RemoteHDTBuilder::new("root.zarr")
        .rdf_path("examples/turtle/rdf.ttl")
        .array_name("array_name")
        .reference_system(ReferenceSystem::PSO)
        .build()
        .serialize();
}
