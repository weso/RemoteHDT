use remote_hdt::{
    engine::EngineStrategy,
    storage::{matrix::MatrixLayout, tabular::TabularLayout, ChunkingStrategy, LocalStorage},
};
use sprs::CsVec;

mod common;

#[test]
fn get_subject_matrix_chunk_test() {
    common::setup(common::GET_OBJECT_MATRIX_ZARR);

    let mut zarr = LocalStorage::new(MatrixLayout);
    let _ = zarr.serialize(
        common::GET_OBJECT_MATRIX_ZARR,
        "resources/rdf.nt",
        ChunkingStrategy::Chunk,
    );
    let dictionary = zarr.get_dictionary();
    let actual = zarr
        .load(common::GET_OBJECT_MATRIX_ZARR)
        .unwrap()
        .get_object(common::Object::Alan.get_idx(&dictionary))
        .unwrap();

    common::setup(common::GET_OBJECT_MATRIX_ZARR);

    assert_eq!(actual, vec![0, 3, 0, 0, 0])
}

#[test]
fn get_subject_matrix_sharding_test() {
    common::setup(common::GET_OBJECT_SHARDING_ZARR);

    let mut zarr = LocalStorage::new(MatrixLayout);
    let _ = zarr.serialize(
        common::GET_OBJECT_SHARDING_ZARR,
        "resources/rdf.nt",
        ChunkingStrategy::Sharding(3),
    );
    let actual = zarr
        .load(common::GET_OBJECT_SHARDING_ZARR)
        .unwrap()
        .get_object(0)
        .unwrap();

    common::setup(common::GET_OBJECT_SHARDING_ZARR);

    assert_eq!(actual, vec![2, 0, 0, 0, 0])
}

#[test]
fn get_subject_tabular_test() {
    common::setup(common::GET_OBJECT_TABULAR_ZARR);

    let mut zarr = LocalStorage::new(TabularLayout);
    let _ = zarr.serialize(
        common::GET_OBJECT_TABULAR_ZARR,
        "resources/rdf.nt",
        ChunkingStrategy::Chunk,
    );
    let dictionary = zarr.get_dictionary();
    let actual = zarr
        .load_sparse(common::GET_OBJECT_TABULAR_ZARR)
        .unwrap()
        .get_object(common::Object::Alan.get_idx(&dictionary))
        .unwrap();

    common::setup(common::GET_OBJECT_TABULAR_ZARR);

    assert_eq!(actual, CsVec::new(4, vec![1], vec![3]))
}
