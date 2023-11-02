use std::time::Instant;

use remote_hdt::remote_hdt::RemoteHDT;

const BENCHMARKS: [&str; 1] = ["root"];

fn main() {
    let before = Instant::now();
    let _ = RemoteHDT::new().load(format!("{}.zarr", BENCHMARKS[0]).as_str());
    let after = before.elapsed();

    println!("Elapsed time: {:.2?}", after)
}
