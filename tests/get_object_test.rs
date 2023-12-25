use remote_hdt::storage::matrix::MatrixLayout;
use remote_hdt::storage::ops::Ops;
use remote_hdt::storage::ops::OpsFormat;
use remote_hdt::storage::params::ChunkingStrategy;
use remote_hdt::storage::params::ReferenceSystem;
use remote_hdt::storage::params::Serialization;
use remote_hdt::storage::tabular::TabularLayout;
use remote_hdt::storage::LocalStorage;
use sprs::TriMat;
use std::error::Error;

mod common;

#[test]
fn get_object_matrix_sharding_test() -> Result<(), Box<dyn Error>> {
    let mut storage = LocalStorage::new(MatrixLayout, Serialization::Zarr);

    common::setup(
        common::SHARDING_ZARR,
        &mut storage,
        ChunkingStrategy::Sharding(3),
        ReferenceSystem::SPO,
    );

    let actual = match storage
        .load(common::SHARDING_ZARR)?
        .get_object(common::Object::Date.into())?
    {
        OpsFormat::Zarr(actual) => actual,
        _ => unreachable!(),
    };

    if actual == vec![2, 0, 0, 0, 0] {
        Ok(())
    } else {
        println!("{:?}", actual);
        Err(String::from("Expected and actual results are not equals").into())
    }
}

#[test]
fn get_object_tabular_test() -> Result<(), Box<dyn Error>> {
    let mut storage = LocalStorage::new(TabularLayout, Serialization::Sparse);

    common::setup(
        common::TABULAR_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::SPO,
    );

    let actual = match storage
        .load(common::TABULAR_ZARR)?
        .get_object(common::Object::Alan.into())?
    {
        OpsFormat::SparseArray(actual) => actual,
        _ => unreachable!(),
    };

    let mut expected = TriMat::new((4, 9));
    expected.add_triplet(1, 3, 3);
    let expected = expected.to_csc();

    if actual == expected {
        Ok(())
    } else {
        Err(String::from("Expected and actual results are not equals").into())
    }
}
