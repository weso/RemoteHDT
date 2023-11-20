use remote_hdt::{
    engine::EngineStrategy,
    storage::{matrix::MatrixLayout, LocalStorage},
};
use std::time::Instant;

const BENCHMARKS: [&str; 1] = ["10-lubm.zarr"];

fn main() {
    let mut remote_hdt = LocalStorage::new(MatrixLayout);
    let arr = remote_hdt.load(BENCHMARKS[0]).unwrap();
    let index = remote_hdt.get_dictionary().get_subject_idx_unchecked(
        "<http://www.Department0.University0.edu/AssistantProfessor0/Publication0>",
    );

    let before = Instant::now();
    let _ = arr.get_subject(index).unwrap();
    let after = before.elapsed();

    println!("Elapsed time: {:.2?}", after)
}
