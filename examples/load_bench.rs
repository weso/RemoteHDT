use remote_hdt::remote_hdt::RemoteHDTBuilder;
use std::time::Instant;

const BENCHMARKS: [&str; 1] = ["root"];

fn main() {
    let before = Instant::now();
    let arr = RemoteHDTBuilder::new(format!("{}.zarr", BENCHMARKS[0]).as_str())
        .unwrap()
        .build()
        .load()
        .unwrap();
    let after = before.elapsed();

    println!("Elapsed time: {:.2?}", after)
}
