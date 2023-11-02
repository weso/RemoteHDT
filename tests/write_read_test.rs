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
        remote_hdt.get_subject_idx_unchecked("<http://example.org/alan>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/Human>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/instanceOf>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/alan>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/warrington>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/placeOfBirth>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/alan>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/wilmslow>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/placeOfDeath>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/alan>"),
        remote_hdt
            .get_object_idx_unchecked("\"1912-06-23\"^^<http://www.w3.org/2001/XMLSchemadate>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/dateOfBirth>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/alan>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/GCHQ>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/employer>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/warrington>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/uk>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/country>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/wilmslow>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/uk>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/country>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/wilmslow>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/town>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/instanceOf>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/bombe>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/alan>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/discoverer>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/bombe>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/computer>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/instanceOf>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/bombe>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/GCHQ>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/manufacturer>"),
    );

    assert_eq!(actual, CsrMatrix::from(&expected));

    after();
}
