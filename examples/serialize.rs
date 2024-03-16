use remote_hdt::error::RemoteHDTError;
use remote_hdt::storage::layout::matrix::MatrixLayout;
use remote_hdt::storage::params::*;
use remote_hdt::storage::Storage;
use std::env;
use std::time::Instant;

fn main() -> Result<(), RemoteHDTError> {
    let args: Vec<String> = env::args().collect();
    if args.len() <= 3 {
        panic!("Usage: cargo run --example serialize <rdf_path> <zarr_path> <shard_size>");
    }

    let rdf_path = &args[1].as_str();
    let zarr_path = &args[2].as_str();
    let shard_size = &args[3].parse::<u64>().unwrap();

    let before = Instant::now();

    Storage::new(MatrixLayout, Serialization::Zarr).serialize(
        Backend::FileSystem(zarr_path),
        rdf_path,
        ChunkingStrategy::Sharding(*shard_size),
        Optimization::Query,
    )?;

    println!("Elapsed time: {:.2?}", before.elapsed());

    Ok(())
}
