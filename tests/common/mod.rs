#![allow(dead_code)]

use remote_hdt::dictionary::Dictionary;
use remote_hdt::storage::params::Backend;
use remote_hdt::storage::params::ChunkingStrategy;
use remote_hdt::storage::params::Optimization;
use remote_hdt::storage::Storage;
use sprs::CsMat;
use sprs::TriMat;
use std::fs::File;

pub const TABULAR_ZARR: &str = "tests/out/tabular.zarr";
pub const MATRIX_ZARR: &str = "tests/out/matrix.zarr";
pub const SHARDING_ZARR: &str = "tests/out/sharding.zarr";
pub const LARGER_ZARR: &str = "tests/out/larger.zarr";
pub const PSO_ZARR: &str = "tests/out/pso.zarr";
pub const OPS_ZARR: &str = "tests/out/ops.zarr";
pub const TABULAR_PSO_ZARR: &str = "tests/out/tabular_pso.zarr";
pub const TABULAR_OPS_ZARR: &str = "tests/out/tabular_ops.zarr";

pub fn setup<C>(
    path: &str,
    storage: &mut Storage<C>,
    chunking_strategy: ChunkingStrategy,
    optimization: Optimization,
) {
    if File::open(path).is_err() {
        storage
            .serialize(
                Backend::FileSystem(path),
                "resources/rdf.nt",
                chunking_strategy,
                optimization,
            )
            .unwrap();
    } else {
        storage.load(Backend::FileSystem(path)).unwrap();
    }
}

pub enum Subject {
    Alan,
    Warrington,
    Wilmslow,
    Bombe,
}

impl Subject {
    pub(crate) fn get_idx(self, dictionary: &Dictionary) -> usize {
        dictionary.get_subject_idx_unchecked(self.into())
    }
}

impl From<Subject> for &str {
    fn from(value: Subject) -> Self {
        match value {
            Subject::Alan => "<http://example.org/alan>",
            Subject::Warrington => "<http://example.org/warrington>",
            Subject::Wilmslow => "<http://example.org/wilmslow>",
            Subject::Bombe => "<http://example.org/bombe>",
        }
    }
}

pub enum Predicate {
    InstanceOf,
    PlaceOfBirth,
    PlaceOfDeath,
    DateOfBirth,
    Employer,
    Country,
    Discoverer,
    Manufacturer,
}

impl Predicate {
    pub(crate) fn get_idx(self, dictionary: &Dictionary) -> usize {
        dictionary.get_predicate_idx_unchecked(self.into())
    }
}

impl From<Predicate> for &str {
    fn from(value: Predicate) -> Self {
        match value {
            Predicate::InstanceOf => "<http://example.org/instanceOf>",
            Predicate::PlaceOfBirth => "<http://example.org/placeOfBirth>",
            Predicate::PlaceOfDeath => "<http://example.org/placeOfDeath>",
            Predicate::DateOfBirth => "<http://example.org/dateOfBirth>",
            Predicate::Employer => "<http://example.org/employer>",
            Predicate::Country => "<http://example.org/country>",
            Predicate::Discoverer => "<http://example.org/discoverer>",
            Predicate::Manufacturer => "<http://example.org/manufacturer>",
        }
    }
}

pub enum Object {
    Human,
    Warrington,
    Wilmslow,
    Date,
    GCHQ,
    UK,
    Town,
    Alan,
    Computer,
}

impl Object {
    pub(crate) fn get_idx(self, dictionary: &Dictionary) -> usize {
        dictionary.get_object_idx_unchecked(self.into())
    }
}

impl From<Object> for &str {
    fn from(value: Object) -> Self {
        match value {
            Object::Human => "<http://example.org/Human>",
            Object::Warrington => "<http://example.org/warrington>",
            Object::Wilmslow => "<http://example.org/wilmslow>",
            Object::Date => "\"1912-06-23\"^^<http://www.w3.org/2001/XMLSchemadate>",
            Object::GCHQ => "<http://example.org/GCHQ>",
            Object::UK => "<http://example.org/uk>",
            Object::Town => "<http://example.org/town>",
            Object::Alan => "<http://example.org/alan>",
            Object::Computer => "<http://example.org/computer>",
        }
    }
}

pub struct Graph;

impl Graph {
    pub fn new(dictionary: &Dictionary) -> CsMat<usize> {
        let mut ans = TriMat::new((4, 9));

        ans.add_triplet(
            Subject::Alan.get_idx(dictionary),
            Object::Human.get_idx(dictionary),
            Predicate::InstanceOf.get_idx(dictionary),
        );
        ans.add_triplet(
            Subject::Alan.get_idx(dictionary),
            Object::Warrington.get_idx(dictionary),
            Predicate::PlaceOfBirth.get_idx(dictionary),
        );
        ans.add_triplet(
            Subject::Alan.get_idx(dictionary),
            Object::Wilmslow.get_idx(dictionary),
            Predicate::PlaceOfDeath.get_idx(dictionary),
        );
        ans.add_triplet(
            Subject::Alan.get_idx(dictionary),
            Object::Date.get_idx(dictionary),
            Predicate::DateOfBirth.get_idx(dictionary),
        );
        ans.add_triplet(
            Subject::Alan.get_idx(dictionary),
            Object::GCHQ.get_idx(dictionary),
            Predicate::Employer.get_idx(dictionary),
        );
        ans.add_triplet(
            Subject::Warrington.get_idx(dictionary),
            Object::UK.get_idx(dictionary),
            Predicate::Country.get_idx(dictionary),
        );
        ans.add_triplet(
            Subject::Wilmslow.get_idx(dictionary),
            Object::UK.get_idx(dictionary),
            Predicate::Country.get_idx(dictionary),
        );
        ans.add_triplet(
            Subject::Wilmslow.get_idx(dictionary),
            Object::Town.get_idx(dictionary),
            Predicate::InstanceOf.get_idx(dictionary),
        );
        ans.add_triplet(
            Subject::Bombe.get_idx(dictionary),
            Object::Alan.get_idx(dictionary),
            Predicate::Discoverer.get_idx(dictionary),
        );
        ans.add_triplet(
            Subject::Bombe.get_idx(dictionary),
            Object::Computer.get_idx(dictionary),
            Predicate::InstanceOf.get_idx(dictionary),
        );
        ans.add_triplet(
            Subject::Bombe.get_idx(dictionary),
            Object::GCHQ.get_idx(dictionary),
            Predicate::Manufacturer.get_idx(dictionary),
        );
        ans.to_csc()
    }
}

pub fn set_expected_first_term_matrix(
    expected: &mut Vec<u32>,
    subject: Subject,
    predicate: Predicate,
    object: Object,
    dictionary: &Dictionary,
    reference_system: ReferenceSystem,
) {
    let subject_idx = subject.get_idx(dictionary);
    let predicate_idx = predicate.get_idx(dictionary);
    let object_idx = object.get_idx(dictionary);

    match reference_system {
        ReferenceSystem::SPO => expected[object_idx] = predicate_idx as u32,
        ReferenceSystem::SOP => expected[predicate_idx] = object_idx as u32,
        ReferenceSystem::PSO => expected[object_idx] = subject_idx as u32,
        ReferenceSystem::POS => expected[subject_idx] = object_idx as u32,
        ReferenceSystem::OSP => expected[predicate_idx] = subject_idx as u32,
        ReferenceSystem::OPS => expected[subject_idx] = predicate_idx as u32,
    }
}

pub fn set_expected_second_term_matrix(
    expected: &mut Vec<u32>,
    subject: Subject,
    predicate: Predicate,
    object: Object,
    dictionary: &Dictionary,
    reference_system: ReferenceSystem,
) {
    let subject_idx = subject.get_idx(dictionary);
    let predicate_idx = predicate.get_idx(dictionary);
    let object_idx = object.get_idx(dictionary);

    match reference_system {
        ReferenceSystem::SPO => {
            expected[subject_idx * dictionary.objects_size() + object_idx] = predicate_idx as u32
        }
        ReferenceSystem::SOP => {
            expected[subject_idx * dictionary.predicates_size() + predicate_idx] = object_idx as u32
        }
        ReferenceSystem::PSO => {
            expected[predicate_idx * dictionary.objects_size() + object_idx] = subject_idx as u32
        }
        ReferenceSystem::POS => {
            expected[predicate_idx * dictionary.subjects_size() + subject_idx] = object_idx as u32
        }
        ReferenceSystem::OSP => {
            expected[object_idx * dictionary.predicates_size() + predicate_idx] = subject_idx as u32
        }
        ReferenceSystem::OPS => {
            expected[object_idx * dictionary.subjects_size() + subject_idx] = predicate_idx as u32
        }
    }
}

pub fn set_expected_third_term_matrix(
    expected: &mut Vec<u32>,
    subject: Subject,
    predicate: Predicate,
    object: Object,
    dictionary: &Dictionary,
    reference_system: ReferenceSystem,
) {
    let subject_idx = subject.get_idx(dictionary);
    let predicate_idx = predicate.get_idx(dictionary);
    let object_idx = object.get_idx(dictionary);

    match reference_system {
        ReferenceSystem::SPO => expected[subject_idx] = predicate_idx as u32,
        ReferenceSystem::SOP => expected[subject_idx] = object_idx as u32,
        ReferenceSystem::PSO => expected[predicate_idx] = subject_idx as u32,
        ReferenceSystem::POS => expected[predicate_idx] = object_idx as u32,
        ReferenceSystem::OSP => expected[object_idx] = subject_idx as u32,
        ReferenceSystem::OPS => expected[object_idx] = predicate_idx as u32,
    }
}
