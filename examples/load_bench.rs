use remote_hdt::storage::tabular::TabularLayout;
use remote_hdt::storage::LocalStorage;
use std::env;
use std::time::Instant;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() <= 1 {
        panic!("Usage: cargo run --example query_bench <number_of_universities>");
    }
    let number_of_universities: &String = &args[1];
    let zarr_path = format!("{}-lubm", number_of_universities);

    let before = Instant::now();
    LocalStorage::new(TabularLayout)
        .load(format!("{}.zarr", zarr_path).as_str())
        .unwrap();
    let after = before.elapsed();

    println!("Elapsed time: {:.2?}", after)
}
