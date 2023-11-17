use remote_hdt::storage::{Layout, Storage};

pub fn main() {
    let _ = Storage::new(Layout::Tabular)
        .serialize("root.zarr", "examples/ntriples/rdf.nt")
        .unwrap();
}
