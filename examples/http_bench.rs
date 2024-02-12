use remote_hdt::error::RemoteHDTError;
use remote_hdt::storage::layout::matrix::MatrixLayout;
use remote_hdt::storage::ops::Ops;
use remote_hdt::storage::params::{Backend, Serialization};
use remote_hdt::storage::Storage;
use std::time::Instant;

fn main() -> Result<(), RemoteHDTError> {
    let mut binding = Storage::new(MatrixLayout, Serialization::Zarr);
    let arr = binding.load(Backend::HTTP(
        "https://raw.githubusercontent.com/weso/RemoteHDT/master/resources/root.zarr",
    ))?;

    let before = Instant::now();
    arr.get_subject("<http://example.org/alan>")?;

    println!("Elapsed time: {:.2?}", before.elapsed());

    Ok(())
}
