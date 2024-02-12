use remote_hdt::error::RemoteHDTError;
use remote_hdt::storage::layout::matrix::MatrixLayout;
use remote_hdt::storage::ops::Ops;
use remote_hdt::storage::params::{Backend, Serialization};
use remote_hdt::storage::Storage;
use std::env;
use std::time::Instant;

const SUBJECT: &str = "<http://www.Department0.University0.edu/AssistantProfessor0/Publication0>";

fn main() -> Result<(), RemoteHDTError> {
    let args: Vec<String> = env::args().collect();
    if args.len() <= 1 {
        panic!("Usage: cargo run --example query_bench <number_of_universities>");
    }

    let number_of_universities: &String = &args[1];
    let zarr_path = format!("{}-lubm", number_of_universities);

    let mut binding = Storage::new(MatrixLayout, Serialization::Zarr);
    let arr = binding.load(Backend::FileSystem(format!("{}.zarr", zarr_path).as_str()))?;

    let before = Instant::now();
    arr.get_subject(SUBJECT)?;

    println!("Elapsed time: {:.2?}", before.elapsed());

    Ok(())
}
