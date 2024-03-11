use common::set_expected_first_term_matrix;
use remote_hdt::storage::layout::matrix::MatrixLayout;
use remote_hdt::storage::layout::tabular::TabularLayout;
use remote_hdt::storage::ops::Ops;
use remote_hdt::storage::ops::OpsFormat;
use remote_hdt::storage::params::Backend;
use remote_hdt::storage::params::ChunkingStrategy;
use remote_hdt::storage::params::ReferenceSystem;
use remote_hdt::storage::params::Serialization;
use remote_hdt::storage::Storage;
use sprs::TriMat;
use std::error::Error;

mod common;

#[test]
fn orientation_pso_matrix_test() -> Result<(), Box<dyn Error>> {
    let mut storage = Storage::new(MatrixLayout, Serialization::Zarr);

    common::setup(
        common::PSO_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::PSO,
    );

    let actual = match storage
        .load(Backend::FileSystem(common::PSO_ZARR))?
        .get_predicate(common::Predicate::InstanceOf.into())?
    {
        OpsFormat::Zarr(actual) => actual,
        _ => unreachable!(),
    };

    let mut expected = vec![0u32; storage.get_dictionary().objects_size()];
    set_expected_first_term_matrix(
        &mut expected,
        common::Subject::Alan,
        common::Predicate::InstanceOf,
        common::Object::Human,
        &storage.get_dictionary(),
        ReferenceSystem::PSO,
    );
    set_expected_first_term_matrix(
        &mut expected,
        common::Subject::Wilmslow,
        common::Predicate::InstanceOf,
        common::Object::Town,
        &storage.get_dictionary(),
        ReferenceSystem::PSO,
    );
    set_expected_first_term_matrix(
        &mut expected,
        common::Subject::Bombe,
        common::Predicate::InstanceOf,
        common::Object::Computer,
        &storage.get_dictionary(),
        ReferenceSystem::PSO,
    );

    if actual == expected {
        Ok(())
    } else {
        Err(String::from("Expected and actual results are not equals").into())
    }
}

#[test]
fn orientation_ops_matrix_test() -> Result<(), Box<dyn Error>> {
    let mut storage = Storage::new(MatrixLayout, Serialization::Zarr);

    common::setup(
        common::OPS_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::OPS,
    );

    let actual = match storage
        .load(Backend::FileSystem(common::OPS_ZARR))?
        .get_object(common::Object::Alan.into())?
    {
        OpsFormat::Zarr(actual) => actual,
        _ => unreachable!(),
    };

    let mut expected = vec![0u32; storage.get_dictionary().subjects_size()];
    set_expected_first_term_matrix(
        &mut expected,
        common::Subject::Bombe,
        common::Predicate::Discoverer,
        common::Object::Alan,
        &storage.get_dictionary(),
        ReferenceSystem::OPS,
    );

    if actual == expected {
        Ok(())
    } else {
        println!("{:?}", actual);
        Err(String::from("Expected and actual results are not equals").into())
    }
}

#[test]
fn orientation_pso_tabular_test() -> Result<(), Box<dyn Error>> {
    let mut storage = Storage::new(TabularLayout, Serialization::Sparse);

    common::setup(
        common::TABULAR_PSO_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::PSO,
    );

    let actual = match storage
        .load(Backend::FileSystem(common::TABULAR_PSO_ZARR))?
        .get_predicate(common::Predicate::InstanceOf.into())?
    {
        OpsFormat::SparseArray(actual) => actual,
        _ => unreachable!(),
    };

    let mut expected = TriMat::new((
        storage.get_dictionary().predicates_size(),
        storage.get_dictionary().objects_size(),
    ));
    expected.add_triplet(
        common::Predicate::InstanceOf.get_idx(&storage.get_dictionary()),
        common::Object::Human.get_idx(&storage.get_dictionary()),
        common::Subject::Alan.get_idx(&storage.get_dictionary()),
    );
    expected.add_triplet(
        common::Predicate::InstanceOf.get_idx(&storage.get_dictionary()),
        common::Object::Town.get_idx(&storage.get_dictionary()),
        common::Subject::Wilmslow.get_idx(&storage.get_dictionary()),
    );
    expected.add_triplet(
        common::Predicate::InstanceOf.get_idx(&storage.get_dictionary()),
        common::Object::Computer.get_idx(&storage.get_dictionary()),
        common::Subject::Bombe.get_idx(&storage.get_dictionary()),
    );

    if actual == expected.to_csc() {
        Ok(())
    } else {
        println!("{:?}", actual);
        Err(String::from("Expected and actual results are not equals").into())
    }
}

#[test]
fn orientation_ops_tabular_test() -> Result<(), Box<dyn Error>> {
    let mut storage = Storage::new(TabularLayout, Serialization::Sparse);

    common::setup(
        common::TABULAR_OPS_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::OPS,
    );

    let actual = match storage
        .load(Backend::FileSystem(common::TABULAR_OPS_ZARR))?
        .get_subject(common::Subject::Alan.into())?
    {
        OpsFormat::SparseArray(actual) => actual,
        _ => unreachable!(),
    };

    let mut expected = TriMat::new((
        storage.get_dictionary().objects_size(),
        storage.get_dictionary().subjects_size(),
    ));
    expected.add_triplet(
        common::Object::Human.get_idx(&storage.get_dictionary()),
        common::Subject::Alan.get_idx(&storage.get_dictionary()),
        common::Predicate::InstanceOf.get_idx(&storage.get_dictionary()),
    );
    expected.add_triplet(
        common::Object::Warrington.get_idx(&storage.get_dictionary()),
        common::Subject::Alan.get_idx(&storage.get_dictionary()),
        common::Predicate::PlaceOfBirth.get_idx(&storage.get_dictionary()),
    );
    expected.add_triplet(
        common::Object::Wilmslow.get_idx(&storage.get_dictionary()),
        common::Subject::Alan.get_idx(&storage.get_dictionary()),
        common::Predicate::PlaceOfDeath.get_idx(&storage.get_dictionary()),
    );
    expected.add_triplet(
        common::Object::Date.get_idx(&storage.get_dictionary()),
        common::Subject::Alan.get_idx(&storage.get_dictionary()),
        common::Predicate::DateOfBirth.get_idx(&storage.get_dictionary()),
    );
    expected.add_triplet(
        common::Object::GCHQ.get_idx(&storage.get_dictionary()),
        common::Subject::Alan.get_idx(&storage.get_dictionary()),
        common::Predicate::Employer.get_idx(&storage.get_dictionary()),
    );

    if actual == expected.to_csc() {
        Ok(())
    } else {
        Err(String::from("Expected and actual results are not equals").into())
    }
}
