use remote_hdt::storage::{matrix::MatrixLayout, ChunkingStrategy, LocalStorage};
use std::time::Instant;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOCATOR: jemallocator::Jemalloc = jemallocator::Jemalloc;

const BENCHMARKS: [&str; 1] = ["10-lubm"];

fn main() {
    let before = Instant::now();

    let _ = LocalStorage::new(MatrixLayout)
        .serialize(
            format!("{}.zarr", BENCHMARKS[0]).as_str(),
            format!("../lubm-uba-improved/out/{}.ttl", BENCHMARKS[0]).as_str(),
            ChunkingStrategy::Chunk,
        )
        .unwrap();

    println!("Elapsed time: {:.2?}", before.elapsed())
}
