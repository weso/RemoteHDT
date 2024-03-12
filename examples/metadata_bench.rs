use remote_hdt::error::RemoteHDTError;
use remote_hdt::metadata::Metadata;
use remote_hdt::storage::params::Backend;
use remote_hdt::storage::params::ReferenceSystem;
use remote_hdt::storage::params::Serialization;
use remote_hdt::metadata::structure::coordinates::CoordinatesStructure;


use remote_hdt::storage::params::ChunkingStrategy;

fn main() -> Result<(), RemoteHDTError> {
    //let rdf_path = "resources/1-lubm.ttl";
    let metadata_path = "resources/metadata_coordinates.csv";
    let zarr_path = "1-lubm-metadata.zarr";
    let fields = vec!["ID","X_pos", "Y_pos"]; // TODO: fix error when the first string is bigger example => vec!["triple_id","X_pos", "Y_pos"] 
    let mut metadata = Metadata::new( CoordinatesStructure,Serialization::Zarr);
    metadata
        .serialize(
            Backend::FileSystem(zarr_path),
            metadata_path, 
            ChunkingStrategy::Sharding(250),
            fields)
        .unwrap();

    Ok(())
}
