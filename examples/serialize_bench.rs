use std::time::Instant;

use remote_hdt::remote_hdt::RemoteHDT;

const BENCHMARKS: [&str; 1] = ["1-lubm"];

fn main() {
    let before = Instant::now();

    let _ = RemoteHDT::new().serialize(
        format!("{}.zarr", BENCHMARKS[0]).as_str(),
        format!("resources/{}.ttl", BENCHMARKS[0]).as_str(),
    );

    println!("Elapsed time: {:.2?}", before.elapsed())
}
