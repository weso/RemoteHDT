use remote_hdt::storage::matrix::MatrixLayout;
use remote_hdt::storage::ops::Ops;
use remote_hdt::storage::ops::OpsFormat;
use remote_hdt::storage::params::ChunkingStrategy;
use remote_hdt::storage::params::ReferenceSystem;
use remote_hdt::storage::params::Serialization;
use remote_hdt::storage::tabular::TabularLayout;
use remote_hdt::storage::LocalStorage;
use std::error::Error;

mod common;

#[test]
fn orientation_pso_matrix_test() -> Result<(), Box<dyn Error>> {
    let mut storage = LocalStorage::new(MatrixLayout, Serialization::Zarr);

    common::setup(
        common::PSO_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::PSO,
    );

    let actual = match storage
        .load(common::PSO_ZARR)?
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
    let mut storage = LocalStorage::new(MatrixLayout, Serialization::Zarr);

    common::setup(
        common::OPS_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::OPS,
    );

    let actual = match storage
        .load(common::OPS_ZARR)?
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
    let mut storage = LocalStorage::new(TabularLayout, Serialization::Sparse);

    common::setup(
        common::TABULAR_PSO_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::PSO,
    );

    let actual = match storage
        .load(common::TABULAR_PSO_ZARR)?
        .get_predicate(common::Predicate::InstanceOf.into())?
    {
        OpsFormat::SparseArray(actual) => actual,
        _ => unreachable!(),
    };

    println!("{}", storage.get_sparse_array().unwrap().to_dense());

    storage
        .get_dictionary()
        .subjects()
        .iter()
        .for_each(|(i, e)| println!("{} {}", i, std::str::from_utf8(&e).unwrap().to_string()));

    println!();

    storage
        .get_dictionary()
        .predicates()
        .iter()
        .for_each(|(i, e)| println!("{} {}", i, std::str::from_utf8(&e).unwrap().to_string()));

    println!();

    storage
        .get_dictionary()
        .objects()
        .iter()
        .for_each(|(i, e)| println!("{} {}", i, std::str::from_utf8(&e).unwrap().to_string()));

    println!(
        "{:?}",
        storage
            .get_dictionary()
            .get_subject_idx(common::Subject::Warrington.into())
    );

    Ok(())

    // if actual == vec![3, 1, 1] {
    //     Ok(())
    // } else {
    //     println!("{:?}", actual);
    //     Err(String::from("Expected and actual results are not equals").into())
    // }
}

#[test]
fn orientation_ops_tabular_test() -> Result<(), Box<dyn Error>> {
    let mut storage = LocalStorage::new(TabularLayout, Serialization::Zarr);

    common::setup(
        common::TABULAR_OPS_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::OPS,
    );

    let actual = match storage
        .load(common::TABULAR_OPS_ZARR)?
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
