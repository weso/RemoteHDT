use remote_hdt::remote_hdt::RemoteHDTBuilder;

pub fn main() {
    let _ = RemoteHDTBuilder::new("root.zarr")
        .unwrap()
        .rdf_path("examples/turtle/rdf.ttl")
        .build()
        .serialize();
}
