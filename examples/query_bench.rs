use remote_hdt::{
    engine::EngineStrategy,
    storage::{Layout, Storage},
};
use std::time::Instant;

const BENCHMARKS: [&str; 1] = ["1-lubm.zarr"];

fn main() {
    let mut remote_hdt = Storage::new(Layout::Tabular);
    let arr = remote_hdt.load(BENCHMARKS[0]).unwrap();
    let index = remote_hdt
        .get_dictionary()
        .get_subject_idx_unchecked("<http://www.Department14.University0.edu/GraduateStudent94>");

    let before = Instant::now();
    let _ = arr.get_subject(vec![index]).unwrap();
    let after = before.elapsed();

    println!("Elapsed time: {:.2?}", after);

    let before = Instant::now();
    let _ = arr.get_neighborhood(0);
    let after = before.elapsed();

    println!("Elapsed time: {:.2?}", after)
}
