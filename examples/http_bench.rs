use remote_hdt::{engine::EngineStrategy, remote_hdt::RemoteHDT};
use std::time::Instant;

const BENCHMARKS: [&str; 1] = ["resources/root.zarr"];

fn main() {
    let mut remote_hdt = RemoteHDT::new();
    let arr = remote_hdt.connect(BENCHMARKS[0]).unwrap();
    let index = remote_hdt.get_subject_idx_unchecked("<http://example.org/alan>");

    let before = Instant::now();
    let arr = arr.get_subject(vec![index]).unwrap();
    let after = before.elapsed();

    println!("Result: {:?}", arr.values());
    println!("Elapsed time: {:.2?}", after)
}
