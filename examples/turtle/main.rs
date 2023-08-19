use remote_hdt::remote_hdt::RemoteHDTBuilder;

pub fn main() {
    RemoteHDTBuilder::new("examples/turtle/rdf.ttl", "root.zarr")
        .array_name("array_name")
        .build()
        .load()
        .unwrap()
}
