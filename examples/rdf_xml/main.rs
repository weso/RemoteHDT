use remote_hdt::error::RemoteHDTError;
use remote_hdt::storage::layout::tabular::TabularLayout;
use remote_hdt::storage::params::*;
use remote_hdt::storage::Storage;

pub fn main() -> Result<(), RemoteHDTError> {
    Storage::new(TabularLayout, Serialization::Zarr).serialize(
        Backend::FileSystem("root.zarr"),
        "examples/rdf_xml/rdf.rdf",
        ChunkingStrategy::Chunk,
        Optimization::Storage(ReferenceSystem::SPO),
    )?;

    Ok(())
}
