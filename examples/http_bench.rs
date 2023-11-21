use remote_hdt::engine::EngineStrategy;
use remote_hdt::storage::matrix::MatrixLayout;
use remote_hdt::storage::HTTPStorage;
use std::time::Instant;

fn main() {
    let mut remote_hdt = HTTPStorage::new(MatrixLayout);
    let arr = remote_hdt
        .connect("https://raw.githubusercontent.com/weso/RemoteHDT/master/resources/root.zarr")
        .unwrap();
    let index = remote_hdt
        .get_dictionary()
        .get_subject_idx_unchecked("<http://example.org/alan>");

    let before = Instant::now();
    arr.get_subject(index).unwrap();
    let after = before.elapsed();

    println!("Elapsed time: {:.2?}", after)
}
