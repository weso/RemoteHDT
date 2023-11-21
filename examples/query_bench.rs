use remote_hdt::engine::EngineStrategy;
use remote_hdt::storage::matrix::MatrixLayout;
use remote_hdt::storage::LocalStorage;
use std::env;
use std::time::Instant;

const SUBJECT: &str = "<http://www.Department0.University0.edu/AssistantProfessor0/Publication0>";

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() <= 1 {
        panic!("Usage: cargo run --example query_bench <number_of_universities>");
    }
    let number_of_universities: &String = &args[1];
    let zarr_path = format!("{}-lubm", number_of_universities);

    let mut remote_hdt = LocalStorage::new(MatrixLayout);
    let arr = remote_hdt
        .load(format!("{}.zarr", zarr_path).as_str())
        .unwrap();
    let index = remote_hdt
        .get_dictionary()
        .get_subject_idx_unchecked(SUBJECT);

    let before = Instant::now();
    arr.get_subject(index).unwrap();
    let after = before.elapsed();

    println!("Elapsed time: {:.2?}", after)
}
