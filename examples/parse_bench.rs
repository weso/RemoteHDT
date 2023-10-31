use remote_hdt::{reference_system::ReferenceSystem, remote_hdt::RemoteHDTBuilder};
use std::time::Instant;

const BENCHMARKS: [&str; 1] = ["1-lubm"];

fn main() {
    let before = Instant::now();

    let _ = RemoteHDTBuilder::new(format!("{}.zarr", BENCHMARKS[0]).as_str())
        .unwrap()
        .reference_system(ReferenceSystem::SPO)
        .array_name("array_name")
        .build()
        .parse();

    println!("Elapsed time: {:.2?}", before.elapsed())
}
