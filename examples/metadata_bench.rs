use remote_hdt::error::RemoteHDTError;
use remote_hdt::metadata::Metadata;
use remote_hdt::storage::params::Serialization;
use remote_hdt::storage::params::Backend;
use remote_hdt::storage::params::ReferenceSystem;

fn main() -> Result<(), RemoteHDTError> {
 
    let rdf_path  = "";
    let metadata_path = "";
    let fields = vec!["X_pos", "Y_pos"];
    let mut metadata: Metadata = Metadata::new(Serialization::Zarr);
    metadata.serialize(rdf_path, ReferenceSystem::SPO,metadata_path,fields).unwrap();

    Ok(())
}
