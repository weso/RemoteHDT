use remote_hdt::storage::matrix::MatrixLayout;
use remote_hdt::storage::params::ChunkingStrategy;
use remote_hdt::storage::params::ReferenceSystem;
use remote_hdt::storage::params::Serialization;
use remote_hdt::storage::tabular::TabularLayout;
use remote_hdt::storage::LocalStorage;

mod common;

#[test]
fn write_read_tabular_test() {
    let mut storage = LocalStorage::new(TabularLayout, Serialization::Sparse);

    common::setup(
        common::MATRIX_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::SPO,
    );

    storage.load(common::TABULAR_ZARR).unwrap();

    assert_eq!(
        storage.get_sparse_array().unwrap(),
        common::Graph::new(&storage.get_dictionary())
    )
}

#[test]
fn write_read_matrix_test() {
    let mut storage = LocalStorage::new(MatrixLayout, Serialization::Sparse);
    common::setup(
        common::MATRIX_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::SPO,
    );

    storage.load(common::MATRIX_ZARR).unwrap();

    assert_eq!(
        storage.get_sparse_array().unwrap(),
        common::Graph::new(&storage.get_dictionary())
    )
}

#[test]
fn write_read_matrix_sharding_test() {
    let mut storage = LocalStorage::new(MatrixLayout, Serialization::Sparse);

    common::setup(
        common::SHARDING_ZARR,
        &mut storage,
        ChunkingStrategy::Sharding(3),
        ReferenceSystem::SPO,
    );

    storage.load(common::SHARDING_ZARR).unwrap();

    assert_eq!(
        storage.get_sparse_array().unwrap(),
        common::Graph::new(&storage.get_dictionary())
    )
}

#[test]
fn write_read_larger_than_triples_shard_test() {
    let mut storage = LocalStorage::new(MatrixLayout, Serialization::Sparse);

    common::setup(
        common::LARGER_ZARR,
        &mut storage,
        ChunkingStrategy::Sharding(10000),
        ReferenceSystem::SPO,
    );

    storage.load(common::LARGER_ZARR).unwrap();

    assert_eq!(
        storage.get_sparse_array().unwrap(),
        common::Graph::new(&storage.get_dictionary())
    )
}
