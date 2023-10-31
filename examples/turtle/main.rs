use remote_hdt::{reference_system::ReferenceSystem, remote_hdt::RemoteHDTBuilder};

pub fn main() {
    let _ = RemoteHDTBuilder::new("root.zarr")
        .unwrap()
        .rdf_path("examples/turtle/rdf.ttl")
        .array_name("array_name")
        .reference_system(ReferenceSystem::PSO)
        .build()
        .serialize();
}
