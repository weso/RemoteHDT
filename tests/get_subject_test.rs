use remote_hdt::{
    engine::EngineStrategy,
    storage::{matrix::MatrixLayout, tabular::TabularLayout, ChunkingStrategy, LocalStorage},
};
use sprs::TriMat;
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

    let mut result = TriMat::new((4, 9));
    result.add_triplet(0, 0, 2);
    result.add_triplet(0, 1, 4);
    result.add_triplet(0, 2, 5);
    result.add_triplet(0, 7, 7);
    result.add_triplet(0, 8, 8);
    let result = result.to_csc();
    assert_eq!(actual, result)
}
