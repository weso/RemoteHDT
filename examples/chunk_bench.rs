use remote_hdt::{engine::EngineStrategy, remote_hdt::RemoteHDTBuilder};
use std::time::Instant;

const BENCHMARKS: [&str; 1] = ["10-lubm"];

fn main() {
    let path = format!("{}.zarr", BENCHMARKS[0]);
    let remote_hdt = RemoteHDTBuilder::new(path.as_str())
        .unwrap()
        .build()
        .parse()
        .unwrap();

    let before = Instant::now();

    let subject = remote_hdt.get_subject(0);

    println!("Elapsed time: {:.2?}", before.elapsed());
    println!("{:?}", subject)
}
