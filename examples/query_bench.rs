use remote_hdt::{engine::EngineStrategy, remote_hdt::RemoteHDTBuilder};
use std::time::Instant;

const BENCHMARKS: [&str; 1] = ["1-lubm.zarr"];

fn main() {
    let mut remote_hdt = RemoteHDTBuilder::new(BENCHMARKS[0]).unwrap().build();
    let arr = remote_hdt.load().unwrap();
    let index = remote_hdt
        .get_subject_idx_unchecked("<http://www.Department14.University0.edu/GraduateStudent94>");

    let before = Instant::now();
    let arr = arr.get_subject(vec![index]).unwrap();
    let after = before.elapsed();

    println!("Result: {:?}", arr.values());
    println!("Elapsed time: {:.2?}", after)
}
