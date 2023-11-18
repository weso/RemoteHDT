use remote_hdt::storage::{Layout, Storage};

mod common;

#[test]
fn write_read_test_tabular() {
    common::setup(common::TABULAR_ZARR);

    let mut zarr = Storage::new(Layout::Tabular);
    let _ = zarr.serialize(common::TABULAR_ZARR, "resources/rdf.nt");
    let dictionary = zarr.get_dictionary();
    let actual = zarr.load_sparse(common::TABULAR_ZARR).unwrap();

    assert_eq!(actual, common::Graph::new(&dictionary));

    common::setup(common::TABULAR_ZARR);
}

#[test]
fn write_read_test_matrix() {
    common::setup(common::MATRIX_ZARR);

    let mut zarr = Storage::new(Layout::Matrix);
    let _ = zarr.serialize(common::MATRIX_ZARR, "resources/rdf.nt");
    let dictionary = zarr.get_dictionary();
    let actual = zarr.load_sparse(common::MATRIX_ZARR).unwrap();

    assert_eq!(actual, common::Graph::new(&dictionary));

    common::setup(common::MATRIX_ZARR);
}
