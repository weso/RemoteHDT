use remote_hdt::{
    engine::EngineStrategy,
    storage::{matrix::MatrixLayout, tabular::TabularLayout, ChunkingStrategy, LocalStorage},
};
use sprs::CsVec;

mod common;

#[test]
fn get_object_matrix_chunk_test() {
    let mut storage = LocalStorage::new(MatrixLayout);
    common::setup(common::MATRIX_ZARR, &mut storage, ChunkingStrategy::Chunk);

    let actual = storage
        .load(common::MATRIX_ZARR)
        .unwrap()
        .get_object(common::Object::Alan.get_idx(&storage.get_dictionary()))
        .unwrap();

    assert_eq!(actual, vec![0, 3, 0, 0, 0])
}

#[test]
fn get_object_matrix_sharding_test() {
    let mut storage = LocalStorage::new(MatrixLayout);
    common::setup(
        common::SHARDING_ZARR,
        &mut storage,
        ChunkingStrategy::Sharding(3),
    );

    let actual = storage
        .load(common::MATRIX_ZARR)
        .unwrap()
        .get_object(0)
        .unwrap();

    assert_eq!(actual, vec![2, 0, 0, 0, 0])
}

#[test]
fn get_object_tabular_test() {
    let mut storage = LocalStorage::new(TabularLayout);
    common::setup(common::TABULAR_ZARR, &mut storage, ChunkingStrategy::Chunk);

    let actual = storage
        .load_sparse(common::TABULAR_ZARR)
        .unwrap()
        .get_object(common::Object::Alan.get_idx(&storage.get_dictionary()))
        .unwrap();

    assert_eq!(actual, CsVec::new(4, vec![1], vec![3]))
}
