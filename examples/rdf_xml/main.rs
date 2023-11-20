use remote_hdt::storage::{tabular::TabularLayout, ChunkingStrategy, LocalStorage};

pub fn main() {
    LocalStorage::new(TabularLayout)
        .serialize(
            "root.zarr",
            "examples/rdf_xml/rdf.rdf",
            ChunkingStrategy::Chunk,
        )
        .unwrap();
}
