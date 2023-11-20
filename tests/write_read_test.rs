use remote_hdt::storage::{
    matrix::MatrixLayout, tabular::TabularLayout, ChunkingStrategy, LocalStorage,
};

mod common;

#[test]
fn write_read_test_tabular() {
    common::setup(common::TABULAR_ZARR);

    let mut zarr = LocalStorage::new(TabularLayout);
    let _ = zarr.serialize(
        common::TABULAR_ZARR,
        "resources/rdf.nt",
        ChunkingStrategy::Chunk,
    );
    let dictionary = zarr.get_dictionary();
    let actual = zarr.load_sparse(common::TABULAR_ZARR).unwrap();

    common::setup(common::TABULAR_ZARR);

    assert_eq!(actual, common::Graph::new(&dictionary))
}

#[test]
fn write_read_test_matrix() {
    common::setup(common::MATRIX_ZARR);

    let mut zarr = LocalStorage::new(MatrixLayout);
    let _ = zarr.serialize(
        common::MATRIX_ZARR,
        "resources/rdf.nt",
        ChunkingStrategy::Chunk,
    );
    let dictionary = zarr.get_dictionary();
    let actual = zarr.load_sparse(common::MATRIX_ZARR).unwrap();

    common::setup(common::MATRIX_ZARR);

    assert_eq!(actual, common::Graph::new(&dictionary))
}

#[test]
fn write_read_matrix_tabular_test() {
    common::setup(common::SHARDING_TABULAR_ZARR);

    let mut zarr = LocalStorage::new(TabularLayout);
    let _ = zarr.serialize(
        common::SHARDING_TABULAR_ZARR,
        "resources/rdf.nt",
        ChunkingStrategy::Sharding(3),
    );
    let dictionary = zarr.get_dictionary();
    let actual = zarr.load_sparse(common::SHARDING_TABULAR_ZARR).unwrap();

    common::setup(common::SHARDING_TABULAR_ZARR);

    assert_eq!(actual, common::Graph::new(&dictionary))
}

#[test]
fn write_read_matrix_sharding_test() {
    common::setup(common::SHARDING_MATRIX_ZARR);

    let mut zarr = LocalStorage::new(MatrixLayout);
    let _ = zarr.serialize(
        common::SHARDING_MATRIX_ZARR,
        "resources/rdf.nt",
        ChunkingStrategy::Sharding(3),
    );
    let dictionary = zarr.get_dictionary();
    let actual = zarr.load_sparse(common::SHARDING_MATRIX_ZARR).unwrap();

    common::setup(common::SHARDING_MATRIX_ZARR);

    assert_eq!(actual, common::Graph::new(&dictionary))
}

#[test]
fn write_read_larger_than_triples_shard_test() {
    common::setup(common::LARGER_ZARR);

    let mut zarr = LocalStorage::new(TabularLayout);
    let _ = zarr.serialize(
        common::LARGER_ZARR,
        "resources/rdf.nt",
        ChunkingStrategy::Sharding(1000),
    );
    let dictionary = zarr.get_dictionary();
    let actual = zarr.load_sparse(common::LARGER_ZARR).unwrap();

    common::setup(common::LARGER_ZARR);

    assert_eq!(actual, common::Graph::new(&dictionary))
}
