use remote_hdt::remote_hdt::RemoteHDTBuilder;

pub fn main() {
    let _ = RemoteHDTBuilder::new("root.zarr")
        .unwrap()
        .rdf_path("examples/ntriples/rdf.nt")
        .build()
        .serialize()
        .unwrap();
}
