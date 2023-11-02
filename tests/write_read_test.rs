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

    let mut remote_hdt = RemoteHDTBuilder::new("root.zarr")
        .unwrap()
        .rdf_path("resources/rdf.nt")
        .build();

    let actual = remote_hdt.load().unwrap();

    let mut expected = CooMatrix::<u8>::new(4, 9);
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/alan>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/instanceOf>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/Human>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/alan>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/placeOfBirth>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/warrington>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/alan>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/placeOfDeath>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/wilmslow>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/alan>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/dateOfBirth>"),
        remote_hdt
            .get_object_idx_unchecked("\"1912-06-23\"^^<http://www.w3.org/2001/XMLSchemadate>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/alan>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/employer>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/GCHQ>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/warrington>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/country>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/uk>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/wilmslow>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/country>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/uk>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/wilmslow>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/instanceOf>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/town>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/bombe>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/discoverer>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/alan>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/bombe>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/instanceOf>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/computer>"),
    );
    expected.push(
        remote_hdt.get_subject_idx_unchecked("<http://example.org/bombe>"),
        remote_hdt.get_predicate_idx_unchecked("<http://example.org/manufacturer>"),
        remote_hdt.get_object_idx_unchecked("<http://example.org/GCHQ>"),
    );

    assert_eq!(actual, CsrMatrix::from(&expected));

    after();
}
