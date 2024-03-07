use remote_hdt::storage::layout::matrix::MatrixLayout;
use remote_hdt::storage::layout::tabular::TabularLayout;
use remote_hdt::storage::ops::Ops;
use remote_hdt::storage::ops::OpsFormat;
use remote_hdt::storage::params::Backend;
use remote_hdt::storage::params::ChunkingStrategy;
use remote_hdt::storage::params::ReferenceSystem;
use remote_hdt::storage::params::Serialization;
use remote_hdt::storage::Storage;
use std::error::Error;

mod common;

#[test]
fn orientation_pso_matrix_test() -> Result<(), Box<dyn Error>> {
    let mut storage = Storage::new(MatrixLayout, Serialization::Zarr);

    common::setup(
        common::PSO_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::PSO,
    );

    let actual = match storage
        .load(Backend::FileSystem(common::PSO_ZARR))?
        .get_predicate(common::Predicate::InstanceOf.into())?
    {
        OpsFormat::Zarr(actual) => actual,
        _ => unreachable!(),
    };

    if actual == vec![3, 0, 1] {
        Ok(())
    } else {
        Err(String::from("Expected and actual results are not equals").into())
    }
}

#[test]
fn orientation_ops_matrix_test() -> Result<(), Box<dyn Error>> {
    let mut storage = Storage::new(MatrixLayout, Serialization::Zarr);

    common::setup(
        common::OPS_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::OPS,
    );

    let actual = match storage
        .load(Backend::FileSystem(common::OPS_ZARR))?
        .get_object(common::Object::Alan.into())?
    {
        OpsFormat::Zarr(actual) => actual,
        _ => unreachable!(),
    };

    if actual == vec![0, 3, 0, 0] {
        Ok(())
    } else {
        println!("{:?}", actual);
        Err(String::from("Expected and actual results are not equals").into())
    }
}

#[test]
fn orientation_pso_tabular_test() -> Result<(), Box<dyn Error>> {
    let mut storage = Storage::new(TabularLayout, Serialization::Sparse);

    common::setup(
        common::TABULAR_PSO_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::PSO,
    );

    let actual = match storage
        .load(Backend::FileSystem(common::TABULAR_PSO_ZARR))?
        .get_predicate(common::Predicate::InstanceOf.into())?
    {
        OpsFormat::Zarr(actual) => actual,
        _ => unreachable!(),
    };

    if actual == vec![3, 1, 1] {
        Ok(())
    } else {
        println!("{:?}", actual);
        Err(String::from("Expected and actual results are not equals").into())
    }
}

#[test]
fn orientation_ops_tabular_test() -> Result<(), Box<dyn Error>> {
    let mut storage = Storage::new(TabularLayout, Serialization::Zarr);

    common::setup(
        common::TABULAR_OPS_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::OPS,
    );

    let actual = match storage
        .load(Backend::FileSystem(common::TABULAR_OPS_ZARR))?
        .get_subject(common::Subject::Alan.into())?
    {
        OpsFormat::Zarr(actual) => actual,
        _ => unreachable!(),
    };

    if actual == vec![1, 3, 4, 0, 0, 0, 0, 6, 7] {
        Ok(())
    } else {
        Err(String::from("Expected and actual results are not equals").into())
    }
}
