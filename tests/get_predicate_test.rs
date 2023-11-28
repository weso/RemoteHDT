use remote_hdt::{
    engine::EngineStrategy,
    storage::{matrix::MatrixLayout, tabular::TabularLayout, ChunkingStrategy, LocalStorage},
};
use sprs::CsVec;

mod common;

#[test]
fn get_predicate_tabular_test() {
    let mut storage = LocalStorage::new(TabularLayout);
    common::setup(common::TABULAR_ZARR, &mut storage, ChunkingStrategy::Chunk);

    let actual = storage
        .load_sparse(common::TABULAR_ZARR)
        .unwrap()
        .get_predicate(common::Predicate::InstanceOf.get_idx(&storage.get_dictionary()) as usize)
        .unwrap();

    assert_eq!(
        actual,
        CsVec::new(9, vec![0, 1, 2, 7, 8], vec![2, 4, 5, 7, 8])
    )
}
