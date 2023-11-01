use remote_hdt::{engine::EngineStrategy, remote_hdt::RemoteHDTBuilder};
use std::time::Instant;

const BENCHMARKS: [&str; 1] = ["10-lubm"];

fn main() {
    let (remote_hdt, _, _, _) = RemoteHDTBuilder::new(format!("{}.zarr", BENCHMARKS[0]).as_str())
        .unwrap()
        .build()
        .load()
        .unwrap();

    let before = Instant::now();

    let _ = remote_hdt.get_object(vec![0, 1, 2, 3, 4, 5]);

    println!("Elapsed time: {:.2?}", before.elapsed())
}
