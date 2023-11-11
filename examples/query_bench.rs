use remote_hdt::engine::EngineStrategy;
use remote_hdt::remote_hdt::RemoteHDT;
use std::time::Instant;

const BENCHMARKS: [&str; 1] = ["100-lubm.zarr"];

fn main() {
    let mut remote_hdt = RemoteHDT::new();
    let arr = remote_hdt.load(BENCHMARKS[0]).unwrap();
    let index = remote_hdt
        .dictionary
        .get_subject_idx_unchecked("<http://www.University0.edu>");

    let before = Instant::now();
    let _ = arr.get_subject(vec![index]).unwrap();
    let after = before.elapsed();

    println!("Elapsed time: {:.2?}", after);

    let before = Instant::now();
    let _ = arr.get_neighborhood(0);
    let after = before.elapsed();

    println!("Elapsed time: {:.2?}", after)
}
