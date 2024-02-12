use remote_hdt::error::RemoteHDTError;
use remote_hdt::storage::layout::tabular::TabularLayout;
use remote_hdt::storage::params::{Backend, ChunkingStrategy, ReferenceSystem, Serialization};
use remote_hdt::storage::Storage;

pub fn main() -> Result<(), RemoteHDTError> {
    Storage::new(TabularLayout, Serialization::Zarr).serialize(
        Backend::FileSystem("root.zarr"),
        "examples/ntriples/rdf.nt",
        ChunkingStrategy::Chunk,
        ReferenceSystem::SPO,
    )?;

    Ok(())
}
