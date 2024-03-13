use remote_hdt::error::RemoteHDTError;
use remote_hdt::metadata::Metadata;
use remote_hdt::storage::params::Backend;
use remote_hdt::storage::params::Serialization;
use remote_hdt::metadata::structure::coordinates::CoordinatesStructure;


use remote_hdt::storage::params::ChunkingStrategy;

fn main() -> Result<(), RemoteHDTError> {
   


    Metadata::new( CoordinatesStructure,Serialization::Zarr).;

 

    Ok(())
}
