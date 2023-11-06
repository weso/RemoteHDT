use std::time::Instant;

use remote_hdt::{remote_hdt::RemoteHDT, utils::print_matrix};

const BENCHMARKS: [&str; 1] = ["100-lubm"];

fn main() {
    let before = Instant::now();
    let arr = RemoteHDT::new().load(format!("{}.zarr", BENCHMARKS[0]).as_str());
    let after = before.elapsed();

    print_matrix(arr.unwrap());
    println!("Elapsed time: {:.2?}", after)
}
