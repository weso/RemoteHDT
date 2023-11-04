use remote_hdt::{engine::EngineStrategy, remote_hdt::RemoteHDT, utils::print_matrix};
use std::time::Instant;

const BENCHMARKS: [&str; 1] =
    ["https://raw.githubusercontent.com/weso/RemoteHDT/master/resources/root.zarr"];

fn main() {
    let mut remote_hdt = RemoteHDT::new();
    let arr = remote_hdt.connect(BENCHMARKS[0]).unwrap();
    let index = remote_hdt
        .dictionary
        .get_subject_idx_unchecked("<http://example.org/alan>");

    let before = Instant::now();
    let arr = arr.get_subject(vec![index]).unwrap();
    let after = before.elapsed();

    print_matrix(arr);
    println!("Elapsed time: {:.2?}", after)
}
