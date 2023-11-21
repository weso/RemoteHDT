use remote_hdt::storage::matrix::MatrixLayout;
use remote_hdt::storage::ChunkingStrategy;
use remote_hdt::storage::LocalStorage;
use std::env;
use std::time::Instant;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() <= 1 {
        panic!("Usage: cargo run --example serialize_bench <number_of_universities>");
    }
    let number_of_universities: &String = &args[1];
    let zarr_path = format!("{}-lubm", number_of_universities);

    let before = Instant::now();

    LocalStorage::new(MatrixLayout)
        .serialize(
            format!("{}.zarr", zarr_path).as_str(),
            format!("../lubm-uba-improved/out/{}.ttl", zarr_path).as_str(),
            ChunkingStrategy::Sharding(1024),
        )
        .unwrap();

    println!("Elapsed time: {:.2?}", before.elapsed())
}
