use remote_hdt::storage::{tabular::TabularLayout, LocalStorage};
use std::time::Instant;

const BENCHMARKS: [&str; 1] = ["100-lubm"];

fn main() {
    let before = Instant::now();
    let _ = LocalStorage::new(TabularLayout).load(format!("{}.zarr", BENCHMARKS[0]).as_str());
    let after = before.elapsed();

    println!("Elapsed time: {:.2?}", after)
}
