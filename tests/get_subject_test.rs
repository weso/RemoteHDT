use remote_hdt::{
    engine::EngineStrategy,
    storage::{matrix::MatrixLayout, tabular::TabularLayout, ChunkingStrategy, LocalStorage},
};
use sprs::CsVec;

mod common;

#[test]
fn get_subject_matrix_chunk_test() {
    let mut storage = LocalStorage::new(MatrixLayout);
    common::setup(common::MATRIX_ZARR, &mut storage, ChunkingStrategy::Chunk);

    let actual = storage
        .load(common::MATRIX_ZARR)
        .unwrap()
        .get_subject(common::Subject::Alan.get_idx(&storage.get_dictionary()))
        .unwrap();

    assert_eq!(actual, vec![2, 4, 5, 0, 0, 0, 0, 7, 8])
}

#[test]
fn get_subject_matrix_sharding_test() {
    let mut storage = LocalStorage::new(MatrixLayout);
    common::setup(
        common::SHARDING_ZARR,
        &mut storage,
        ChunkingStrategy::Sharding(3),
    );

    let actual = storage
        .load(common::SHARDING_ZARR)
        .unwrap()
        .get_subject(3)
        .unwrap();

    assert_eq!(actual, vec![0, 0, 0, 0, 0, 5, 1, 0, 0])
}

#[test]
fn get_subject_tabular_test() {
    let mut storage = LocalStorage::new(TabularLayout);
    common::setup(common::TABULAR_ZARR, &mut storage, ChunkingStrategy::Chunk);

    let actual = storage
        .load_sparse(common::TABULAR_ZARR)
        .unwrap()
        .get_subject(common::Subject::Alan.get_idx(&storage.get_dictionary()))
        .unwrap();

    assert_eq!(
        actual,
        CsVec::new(9, vec![0, 1, 2, 7, 8], vec![2, 4, 5, 7, 8])
    )
}
