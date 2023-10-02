use remote_hdt::remote_hdt::RemoteHDTBuilder;
use std::time::Instant;

const BENCHMARKS: [&str; 1] = ["1-lubm"];

fn main() {
    let before = Instant::now();

    let _ = RemoteHDTBuilder::new(format!("{}.zarr", BENCHMARKS[0]).as_str())
        .rdf_path(format!("../lubm-uba-improved/out/{}.ttl", BENCHMARKS[0]).as_str())
        .array_name("array_name")
        .build()
        .serialize();

    println!("Elapsed time: {:.2?}", before.elapsed())
}
