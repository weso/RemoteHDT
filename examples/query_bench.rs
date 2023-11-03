use remote_hdt::engine::EngineStrategy;
use remote_hdt::remote_hdt::print;
use remote_hdt::remote_hdt::RemoteHDT;
use std::time::Instant;

const BENCHMARKS: [&str; 1] = ["1-lubm.zarr"];

fn main() {
    let mut remote_hdt = RemoteHDT::new();
    let arr = remote_hdt.load(BENCHMARKS[0]).unwrap();
    let index = remote_hdt
        .dictionary
        .get_subject_idx_unchecked("<http://www.Department14.University0.edu/GraduateStudent94>");

    let before = Instant::now();
    let arr = arr.get_subject(vec![index]).unwrap();
    let after = before.elapsed();

    print(arr);
    println!("Elapsed time: {:.2?}", after)
}
