use std::fs::remove_dir_all;

use nalgebra_sparse::{CooMatrix, CsrMatrix};
use remote_hdt::remote_hdt::RemoteHDT;

fn before() {
    let _ = remove_dir_all("root.zarr");
}

fn after() {
    let _ = remove_dir_all("root.zarr");
}

#[test]
fn write_read_test() {
    before();

    let mut remote_hdt = RemoteHDT::new();

    let _ = remote_hdt.serialize("root.zarr", "resources/rdf.nt");

    let actual = remote_hdt.load("root.zarr").unwrap();

    let mut expected = CooMatrix::<u32>::new(18, 18);
    expected.push(
        remote_hdt
            .dictionary
            .get_subject_idx_unchecked("<http://example.org/alan>"),
        remote_hdt
            .dictionary
            .get_object_idx_unchecked("<http://example.org/Human>"),
        remote_hdt
            .dictionary
            .get_predicate_idx_unchecked("<http://example.org/instanceOf>")
            .try_into()
            .unwrap(),
    );
    expected.push(
        remote_hdt
            .dictionary
            .get_subject_idx_unchecked("<http://example.org/alan>"),
        remote_hdt
            .dictionary
            .get_object_idx_unchecked("<http://example.org/warrington>"),
        remote_hdt
            .dictionary
            .get_predicate_idx_unchecked("<http://example.org/placeOfBirth>")
            .try_into()
            .unwrap(),
    );
    expected.push(
        remote_hdt
            .dictionary
            .get_subject_idx_unchecked("<http://example.org/alan>"),
        remote_hdt
            .dictionary
            .get_object_idx_unchecked("<http://example.org/wilmslow>"),
        remote_hdt
            .dictionary
            .get_predicate_idx_unchecked("<http://example.org/placeOfDeath>")
            .try_into()
            .unwrap(),
    );
    expected.push(
        remote_hdt
            .dictionary
            .get_subject_idx_unchecked("<http://example.org/alan>"),
        remote_hdt
            .dictionary
            .get_object_idx_unchecked("\"1912-06-23\"^^<http://www.w3.org/2001/XMLSchemadate>"),
        remote_hdt
            .dictionary
            .get_predicate_idx_unchecked("<http://example.org/dateOfBirth>")
            .try_into()
            .unwrap(),
    );
    expected.push(
        remote_hdt
            .dictionary
            .get_subject_idx_unchecked("<http://example.org/alan>"),
        remote_hdt
            .dictionary
            .get_object_idx_unchecked("<http://example.org/GCHQ>"),
        remote_hdt
            .dictionary
            .get_predicate_idx_unchecked("<http://example.org/employer>")
            .try_into()
            .unwrap(),
    );
    expected.push(
        remote_hdt
            .dictionary
            .get_subject_idx_unchecked("<http://example.org/warrington>"),
        remote_hdt
            .dictionary
            .get_object_idx_unchecked("<http://example.org/uk>"),
        remote_hdt
            .dictionary
            .get_predicate_idx_unchecked("<http://example.org/country>")
            .try_into()
            .unwrap(),
    );
    expected.push(
        remote_hdt
            .dictionary
            .get_subject_idx_unchecked("<http://example.org/wilmslow>"),
        remote_hdt
            .dictionary
            .get_object_idx_unchecked("<http://example.org/uk>"),
        remote_hdt
            .dictionary
            .get_predicate_idx_unchecked("<http://example.org/country>")
            .try_into()
            .unwrap(),
    );
    expected.push(
        remote_hdt
            .dictionary
            .get_subject_idx_unchecked("<http://example.org/wilmslow>"),
        remote_hdt
            .dictionary
            .get_object_idx_unchecked("<http://example.org/town>"),
        remote_hdt
            .dictionary
            .get_predicate_idx_unchecked("<http://example.org/instanceOf>")
            .try_into()
            .unwrap(),
    );
    expected.push(
        remote_hdt
            .dictionary
            .get_subject_idx_unchecked("<http://example.org/bombe>"),
        remote_hdt
            .dictionary
            .get_object_idx_unchecked("<http://example.org/alan>"),
        remote_hdt
            .dictionary
            .get_predicate_idx_unchecked("<http://example.org/discoverer>")
            .try_into()
            .unwrap(),
    );
    expected.push(
        remote_hdt
            .dictionary
            .get_subject_idx_unchecked("<http://example.org/bombe>"),
        remote_hdt
            .dictionary
            .get_object_idx_unchecked("<http://example.org/computer>"),
        remote_hdt
            .dictionary
            .get_predicate_idx_unchecked("<http://example.org/instanceOf>")
            .try_into()
            .unwrap(),
    );
    expected.push(
        remote_hdt
            .dictionary
            .get_subject_idx_unchecked("<http://example.org/bombe>"),
        remote_hdt
            .dictionary
            .get_object_idx_unchecked("<http://example.org/GCHQ>"),
        remote_hdt
            .dictionary
            .get_predicate_idx_unchecked("<http://example.org/manufacturer>")
            .try_into()
            .unwrap(),
    );

    assert_eq!(actual, CsrMatrix::from(&expected));

    after();
}
