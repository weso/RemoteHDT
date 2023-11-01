use remote_hdt::{engine::EngineStrategy, remote_hdt::RemoteHDTBuilder};
use std::time::Instant;

const BENCHMARKS: [&str; 1] = ["1-lubm"];

fn main() {
    let remote_hdt = RemoteHDTBuilder::new(format!("{}.zarr", BENCHMARKS[0]).as_str())
        .unwrap()
        .array_name("array_name")
        .build()
        .parse()
        .unwrap();

    let before = Instant::now();

    let _ = remote_hdt.get_predicate(0);

    println!("Elapsed time: {:.2?}", before.elapsed())
}
