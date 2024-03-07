use remote_hdt::storage::layout::matrix::MatrixLayout;
use remote_hdt::storage::layout::tabular::TabularLayout;
use remote_hdt::storage::ops::Ops;
use remote_hdt::storage::ops::OpsFormat;
use remote_hdt::storage::params::Backend;
use remote_hdt::storage::params::ChunkingStrategy;
use remote_hdt::storage::params::ReferenceSystem;
use remote_hdt::storage::params::Serialization;
use remote_hdt::storage::Storage;
use sprs::TriMat;
use std::error::Error;

mod common;

#[test]
fn get_predicate_matrix_chunk_test() -> Result<(), Box<dyn Error>> {
    let mut storage = Storage::new(MatrixLayout, Serialization::Zarr);

    common::setup(
        common::MATRIX_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::SPO,
    );

    let actual = match storage
        .load(Backend::FileSystem(common::MATRIX_ZARR))?
        .get_predicate(common::Predicate::InstanceOf.into())?
    {
        OpsFormat::Zarr(actual) => actual,
        _ => unreachable!(),
    };

    if actual
        == vec![
            0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 5, 0, 0, 0,
        ]
    {
        Ok(())
    } else {
        Err(String::from("Expected and actual results are not equals").into())
    }
}

#[test]
fn get_predicate_tabular_test() -> Result<(), Box<dyn Error>> {
    let mut storage = Storage::new(TabularLayout, Serialization::Sparse);

    common::setup(
        common::TABULAR_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::SPO,
    );

    let actual = match storage
        .load(Backend::FileSystem(common::TABULAR_ZARR))?
        .get_predicate(common::Predicate::InstanceOf.into())?
    {
        OpsFormat::SparseArray(actual) => actual,
        _ => unreachable!(),
    };

    let mut expected = TriMat::new((4, 9));
    expected.add_triplet(
        common::Subject::Alan.get_idx(&storage.get_dictionary()),
        common::Object::Human.get_idx(&storage.get_dictionary()),
        common::Predicate::InstanceOf.get_idx(&storage.get_dictionary()),
    );
    expected.add_triplet(
        common::Subject::Wilmslow.get_idx(&storage.get_dictionary()),
        common::Object::Town.get_idx(&storage.get_dictionary()),
        common::Predicate::InstanceOf.get_idx(&storage.get_dictionary()),
    );
    expected.add_triplet(
        common::Subject::Bombe.get_idx(&storage.get_dictionary()),
        common::Object::Computer.get_idx(&storage.get_dictionary()),
        common::Predicate::InstanceOf.get_idx(&storage.get_dictionary()),
    );
    let expected = expected.to_csc();

    if actual == expected {
        Ok(())
    } else {
        Err(String::from("Expected and actual results are not equals").into())
    }
}
