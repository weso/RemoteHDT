use remote_hdt::{
    engine::EngineStrategy,
    storage::{Layout, Storage},
};
use std::time::Instant;

const BENCHMARKS: [&str; 1] =
    ["https://raw.githubusercontent.com/weso/RemoteHDT/master/resources/root.zarr"];

fn main() {
    let mut remote_hdt = Storage::new(Layout::Tabular);
    let arr = remote_hdt.connect(BENCHMARKS[0]).unwrap();
    let index = remote_hdt
        .get_dictionary()
        .get_subject_idx_unchecked("<http://example.org/alan>");

    let before = Instant::now();
    let _ = arr.get_subject(index).unwrap();
    let after = before.elapsed();

    println!("Elapsed time: {:.2?}", after)
}
