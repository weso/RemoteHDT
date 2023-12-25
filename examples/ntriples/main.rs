use remote_hdt::error::RemoteHDTError;
use remote_hdt::storage::params::{ChunkingStrategy, ReferenceSystem, Serialization};
use remote_hdt::storage::tabular::TabularLayout;
use remote_hdt::storage::LocalStorage;

pub fn main() -> Result<(), RemoteHDTError> {
    LocalStorage::new(TabularLayout, Serialization::Zarr).serialize(
        "root.zarr",
        "examples/ntriples/rdf.nt",
        ChunkingStrategy::Chunk,
        ReferenceSystem::SPO,
    )?;

    Ok(())
}
