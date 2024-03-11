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
fn get_subject_matrix_chunk_test() -> Result<(), Box<dyn Error>> {
    let mut storage = Storage::new(MatrixLayout, Serialization::Zarr);

    common::setup(
        common::MATRIX_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::SPO,
    );

    let actual = match storage
        .load(Backend::FileSystem(common::MATRIX_ZARR))?
        .get_subject(common::Subject::Alan.into())?
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
        ReferenceSystem::SPO,
    );
    set_expected_first_term_matrix(
        &mut expected,
        common::Subject::Alan,
        common::Predicate::PlaceOfBirth,
        common::Object::Warrington,
        &storage.get_dictionary(),
        ReferenceSystem::SPO,
    );
    set_expected_first_term_matrix(
        &mut expected,
        common::Subject::Alan,
        common::Predicate::PlaceOfDeath,
        common::Object::Wilmslow,
        &storage.get_dictionary(),
        ReferenceSystem::SPO,
    );
    set_expected_first_term_matrix(
        &mut expected,
        common::Subject::Alan,
        common::Predicate::DateOfBirth,
        common::Object::Date,
        &storage.get_dictionary(),
        ReferenceSystem::SPO,
    );
    set_expected_first_term_matrix(
        &mut expected,
        common::Subject::Alan,
        common::Predicate::Employer,
        common::Object::GCHQ,
        &storage.get_dictionary(),
        ReferenceSystem::SPO,
    );

    if actual == expected {
        Ok(())
    } else {
        Err(String::from("Expected and actual results are not equals").into())
    }
}

#[test]
fn get_subject_matrix_sharding_test() -> Result<(), Box<dyn Error>> {
    let mut storage = Storage::new(MatrixLayout, Serialization::Zarr);

    common::setup(
        common::SHARDING_ZARR,
        &mut storage,
        ChunkingStrategy::Sharding(4),
        ReferenceSystem::SPO,
    );

    let actual = match storage
        .load(Backend::FileSystem(common::SHARDING_ZARR))?
        .get_subject(common::Subject::Wilmslow.into())?
    {
        OpsFormat::Zarr(actual) => actual,
        _ => unreachable!(),
    };

    let mut expected = vec![0u32; storage.get_dictionary().objects_size()];
    expected[5] = common::Predicate::InstanceOf.get_idx(&storage.get_dictionary()) as u32;
    expected[6] = common::Predicate::Country.get_idx(&storage.get_dictionary()) as u32;

    if actual == expected {
        Ok(())
    } else {
        Err(String::from("Expected and actual results are not equals").into())
    }
}

#[test]
fn get_subject_tabular_test() -> Result<(), Box<dyn Error>> {
    let mut storage = Storage::new(TabularLayout, Serialization::Sparse);

    common::setup(
        common::TABULAR_ZARR,
        &mut storage,
        ChunkingStrategy::Chunk,
        ReferenceSystem::SPO,
    );

    let actual = match storage
        .load(Backend::FileSystem(common::TABULAR_ZARR))?
        .get_subject(common::Subject::Alan.into())?
    {
        OpsFormat::SparseArray(actual) => actual,
        _ => unreachable!(),
    };

    let mut expected = TriMat::new((
        storage.get_dictionary().subjects_size(),
        storage.get_dictionary().objects_size(),
    ));
    expected.add_triplet(
        common::Subject::Alan.get_idx(&storage.get_dictionary()),
        common::Object::Human.get_idx(&storage.get_dictionary()),
        common::Predicate::InstanceOf.get_idx(&storage.get_dictionary()),
    );
    expected.add_triplet(
        common::Subject::Alan.get_idx(&storage.get_dictionary()),
        common::Object::Warrington.get_idx(&storage.get_dictionary()),
        common::Predicate::PlaceOfBirth.get_idx(&storage.get_dictionary()),
    );
    expected.add_triplet(
        common::Subject::Alan.get_idx(&storage.get_dictionary()),
        common::Object::Wilmslow.get_idx(&storage.get_dictionary()),
        common::Predicate::PlaceOfDeath.get_idx(&storage.get_dictionary()),
    );
    expected.add_triplet(
        common::Subject::Alan.get_idx(&storage.get_dictionary()),
        common::Object::Date.get_idx(&storage.get_dictionary()),
        common::Predicate::DateOfBirth.get_idx(&storage.get_dictionary()),
    );
    expected.add_triplet(
        common::Subject::Alan.get_idx(&storage.get_dictionary()),
        common::Object::GCHQ.get_idx(&storage.get_dictionary()),
        common::Predicate::Employer.get_idx(&storage.get_dictionary()),
    );

    if actual == expected.to_csc() {
        Ok(())
    } else {
        Err(String::from("Expected and actual results are not equals").into())
    }
}
