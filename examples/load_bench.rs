use remote_hdt::error::RemoteHDTError;
use remote_hdt::storage::params::{Backend, Serialization};
use remote_hdt::storage::tabular::TabularLayout;
use remote_hdt::storage::Storage;
use std::env;
use std::time::Instant;

fn main() -> Result<(), RemoteHDTError> {
    let args: Vec<String> = env::args().collect();
    if args.len() <= 1 {
        panic!("Usage: cargo run --example query_bench <number_of_universities>");
    }

    let number_of_universities: &String = &args[1];
    let zarr_path = format!("{}-lubm", number_of_universities);

    let before = Instant::now();

    Storage::new(TabularLayout, Serialization::Zarr)
        .load(Backend::FileSystem(format!("{}.zarr", zarr_path).as_str()))?;

    println!("Elapsed time: {:.2?}", before.elapsed());

    Ok(())
}
