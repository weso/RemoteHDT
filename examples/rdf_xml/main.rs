use remote_hdt::remote_hdt::RemoteHDT;

pub fn main() {
    let _ = RemoteHDT::new().serialize("root.zarr", "examples/rdf_xml/rdf.rdf");
}
