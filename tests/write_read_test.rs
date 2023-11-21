use remote_hdt::storage::{
    matrix::MatrixLayout, tabular::TabularLayout, ChunkingStrategy, LocalStorage,
};

mod common;

#[test]
fn write_read_tabular_test() {
    let mut storage = LocalStorage::new(TabularLayout);
    common::setup(common::MATRIX_ZARR, &mut storage, ChunkingStrategy::Chunk);
    assert_eq!(
        storage.load_sparse(common::TABULAR_ZARR).unwrap(),
        common::Graph::new(&storage.get_dictionary())
    )
}

#[test]
fn write_read_matrix_test() {
    let mut storage = LocalStorage::new(MatrixLayout);
    common::setup(common::MATRIX_ZARR, &mut storage, ChunkingStrategy::Chunk);
    assert_eq!(
        storage.load_sparse(common::MATRIX_ZARR).unwrap(),
        common::Graph::new(&storage.get_dictionary())
    )
}

#[test]
fn write_read_matrix_sharding_test() {
    let mut storage = LocalStorage::new(MatrixLayout);

    common::setup(
        common::SHARDING_ZARR,
        &mut storage,
        ChunkingStrategy::Sharding(3),
    );

    assert_eq!(
        storage.load_sparse(common::SHARDING_ZARR).unwrap(),
        common::Graph::new(&storage.get_dictionary())
    )
}

#[test]
fn write_read_larger_than_triples_shard_test() {
    let mut storage = LocalStorage::new(MatrixLayout);

    common::setup(
        common::LARGER_ZARR,
        &mut storage,
        ChunkingStrategy::Sharding(10000),
    );

    assert_eq!(
        storage.load_sparse(common::LARGER_ZARR).unwrap(),
        common::Graph::new(&storage.get_dictionary())
    )
}
