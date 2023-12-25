use remote_hdt::error::RemoteHDTError;
use remote_hdt::storage::matrix::MatrixLayout;
use remote_hdt::storage::ops::Ops;
use remote_hdt::storage::params::Serialization;
use remote_hdt::storage::HTTPStorage;
use std::time::Instant;

fn main() -> Result<(), RemoteHDTError> {
    let mut remote_hdt = HTTPStorage::new(MatrixLayout, Serialization::Zarr);
    let arr = remote_hdt
        .connect("https://raw.githubusercontent.com/weso/RemoteHDT/master/resources/root.zarr")?;

    let before = Instant::now();
    arr.get_subject("<http://example.org/alan>")?;

    println!("Elapsed time: {:.2?}", before.elapsed());

    Ok(())
}
