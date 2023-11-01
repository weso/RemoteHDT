use std::fs::remove_dir_all;

use nalgebra_sparse::{CooMatrix, CsrMatrix};
use remote_hdt::remote_hdt::RemoteHDTBuilder;

fn before() {
    let _ = remove_dir_all("root.zarr");
}

fn after() {
    let _ = remove_dir_all("root.zarr");
}

#[test]
fn write_read_test() {
    before();

    let _ = RemoteHDTBuilder::new("root.zarr")
        .unwrap()
        .rdf_path("resources/rdf.nt")
        .build()
        .serialize();

    let mut expected = CooMatrix::<u8>::new(4, 9);
    expected.push(0, 0, 0);
    expected.push(0, 1, 1);
    expected.push(0, 2, 2);
    expected.push(0, 3, 3);
    expected.push(0, 4, 4);
    expected.push(1, 5, 5);
    expected.push(2, 5, 5);
    expected.push(2, 6, 0);
    expected.push(3, 7, 6);
    expected.push(3, 8, 0);
    expected.push(3, 4, 7);

    let (actual, _, _, _) = RemoteHDTBuilder::new("root.zarr")
        .unwrap()
        .rdf_path("resources/rdf.nt")
        .build()
        .load()
        .unwrap();

    assert_eq!(actual, CsrMatrix::from(&expected));

    after();
}
