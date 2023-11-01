use remote_hdt::remote_hdt::RemoteHDTBuilder;
use std::time::Instant;

const BENCHMARKS: [&str; 1] = ["1-lubm"];

fn main() {
    let before = Instant::now();

    let _ = RemoteHDTBuilder::new(format!("{}.zarr", BENCHMARKS[0]).as_str())
        .unwrap()
        .build()
        .load();

    println!("Elapsed time: {:.2?}", before.elapsed())
}
