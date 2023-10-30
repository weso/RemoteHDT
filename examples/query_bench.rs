use remote_hdt::remote_hdt::{Engine, RemoteHDTBuilder};
use std::time::Instant;

const BENCHMARKS: [&str; 1] = ["1-lubm"];

fn main() {
    let path = format!("{}.zarr", BENCHMARKS[0]);
    let remote_hdt = RemoteHDTBuilder::new(path.as_str())
        .reference_system(remote_hdt::remote_hdt::ReferenceSystem::SPO)
        .array_name("array_name")
        .build()
        .parse()
        .unwrap();

    let before = Instant::now();

    let _ = remote_hdt.get_predicate(0);

    println!("Elapsed time: {:.2?}", before.elapsed())
}
