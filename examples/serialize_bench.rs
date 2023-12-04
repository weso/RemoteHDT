use remote_hdt::storage::matrix::MatrixLayout;
use remote_hdt::storage::ChunkingStrategy;
use remote_hdt::storage::LocalStorage;
use std::env;
use std::time::Instant;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOCATOR: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() <= 3 {
        panic!("Usage: cargo run --example serialize_bench <rdf_path> <zarr_path> <shard_size>");
    }
    let rdf_path: &String = &args[1];
    let zarr_path: &String = &args[2];
    let shard_size: &String = &args[3];
  
    let before = Instant::now();

    LocalStorage::new(MatrixLayout)
        .serialize(
                    &zarr_path.as_str(),
            &rdf_path.as_str(),
            ChunkingStrategy::Sharding(shard_size.parse::<u64>().unwrap()),
        )
        .unwrap();

    println!("Elapsed time: {:.2?}", before.elapsed())
}
