use std::time::Instant;

use remote_hdt::remote_hdt::RemoteHDT;

const BENCHMARKS: [&str; 1] = ["10-lubm"];

fn main() {
    let before = Instant::now();

    let _ = RemoteHDT::new()
        .serialize(
            format!("{}.zarr", BENCHMARKS[0]).as_str(),
            format!("../lubm-uba-improved/out/{}.ttl", BENCHMARKS[0]).as_str(),
        )
        .unwrap();

    println!("Elapsed time: {:.2?}", before.elapsed())
}
