use remote_hdt::error::RemoteHDTError;
use remote_hdt::metadata::Metadata;
use remote_hdt::storage::params::Backend;
use remote_hdt::storage::params::ReferenceSystem;
use remote_hdt::storage::params::Serialization;
use remote_hdt::storage::layout::metadata::MetadataLayout;


use remote_hdt::storage::params::ChunkingStrategy;

fn main() -> Result<(), RemoteHDTError> {
    let rdf_path = "resources/1-lubm.ttl";
    let metadata_path = "";
    let zarr_path = "1-lubm-metadata.zarr";
    let fields = vec!["X_pos", "Y_pos"];
    let mut metadata = Metadata::new( MetadataLayout,Serialization::Zarr);
    metadata
        .serialize(
            Backend::FileSystem(zarr_path),
            rdf_path, 
            ChunkingStrategy::Sharding(1024),
            ReferenceSystem::SPO, 

            metadata_path, 
            fields)
        .unwrap();

    Ok(())
}
