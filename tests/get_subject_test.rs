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
fn get_subject_matrix_chunk_test() -> Result<(), Box<dyn Error>> {
    let mut storage = Storage::new(MatrixLayout, Serialization::Zarr);

    common::setup(
        common::MATRIX_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::SPO,
    );

    let actual = match storage
        .load(Backend::FileSystem(common::MATRIX_ZARR))?
        .get_subject(common::Subject::Alan.into())?
    {
        OpsFormat::Zarr(actual) => actual,
        _ => unreachable!(),
    };

    if actual == vec![2, 4, 5, 0, 0, 0, 0, 7, 8] {
        Ok(())
    } else {
        Err(String::from("Expected and actual results are not equals").into())
    }
}

#[test]
fn get_subject_matrix_sharding_test() -> Result<(), Box<dyn Error>> {
    let mut storage = Storage::new(MatrixLayout, Serialization::Zarr);

    common::setup(
        common::SHARDING_ZARR,
        &mut storage,
        ChunkingStrategy::Sharding(4),
        ReferenceSystem::SPO,
    );

    let actual = match storage
        .load(Backend::FileSystem(common::SHARDING_ZARR))?
        .get_subject(common::Subject::Wilmslow.into())?
    {
        OpsFormat::Zarr(actual) => actual,
        _ => unreachable!(),
    };

    if actual == vec![0, 0, 0, 0, 0, 5, 1, 0, 0] {
        Ok(())
    } else {
        Err(String::from("Expected and actual results are not equals").into())
    }
}

#[test]
fn get_subject_tabular_test() -> Result<(), Box<dyn Error>> {
    let mut storage = Storage::new(TabularLayout, Serialization::Sparse);

    common::setup(
        common::TABULAR_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::SPO,
    );

    let actual = match storage
        .load(Backend::FileSystem(common::TABULAR_ZARR))?
        .get_subject(common::Subject::Alan.into())?
    {
        OpsFormat::SparseArray(actual) => actual,
        _ => unreachable!(),
    };

    let mut expected = TriMat::new((4, 9));
    expected.add_triplet(0, 0, 2);
    expected.add_triplet(0, 1, 4);
    expected.add_triplet(0, 2, 5);
    expected.add_triplet(0, 7, 7);
    expected.add_triplet(0, 8, 8);
    let expected = expected.to_csc();

    if actual == expected {
        Ok(())
    } else {
        Err(String::from("Expected and actual results are not equals").into())
    }
}
