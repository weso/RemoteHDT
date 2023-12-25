use remote_hdt::error::RemoteHDTError;
use remote_hdt::storage::matrix::MatrixLayout;
use remote_hdt::storage::params::{ChunkingStrategy, ReferenceSystem, Serialization};
use remote_hdt::storage::LocalStorage;
use std::env;
use std::time::Instant;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOCATOR: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() -> Result<(), RemoteHDTError> {
    let args: Vec<String> = env::args().collect();
    if args.len() <= 3 {
        panic!("Usage: cargo run --example serialize_bench <rdf_path> <zarr_path> <shard_size>");
    }

    let rdf_path = &args[1].as_str();
    let zarr_path = &args[2].as_str();
    let shard_size = &args[3].parse::<u64>().unwrap();

    let before = Instant::now();

    LocalStorage::new(MatrixLayout, Serialization::Zarr).serialize(
        zarr_path,
        rdf_path,
        ChunkingStrategy::Sharding(*shard_size),
        ReferenceSystem::SPO,
    )?;

    println!("Elapsed time: {:.2?}", before.elapsed());

    Ok(())
}
