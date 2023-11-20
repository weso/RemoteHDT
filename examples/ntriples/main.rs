use remote_hdt::storage::{tabular::TabularLayout, ChunkingStrategy, LocalStorage};

pub fn main() {
    LocalStorage::new(TabularLayout)
        .serialize(
            "root.zarr",
            "examples/ntriples/rdf.nt",
            ChunkingStrategy::Chunk,
        )
        .unwrap();
}
