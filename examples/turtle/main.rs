use remote_hdt::storage::tabular::TabularLayout;
use remote_hdt::storage::ChunkingStrategy;
use remote_hdt::storage::LocalStorage;

pub fn main() {
    LocalStorage::new(TabularLayout)
        .serialize(
            "root.zarr",
            "examples/turtle/rdf.ttk",
            ChunkingStrategy::Chunk,
        )
        .unwrap();
}
