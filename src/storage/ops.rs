use safe_transmute::TriviallyTransmutable;
use zarrs::storage::ReadableStorageTraits;

use crate::engine::EngineStrategy;
use crate::error::OpsError;

use super::params::ReferenceSystem;
use super::params::Serialization;
use super::Storage;
use super::ZarrArray;
use super::ZarrType;

pub type OpsResult = Result<OpsFormat, OpsError>;

pub enum OpsFormat {
    SparseArray(ZarrArray),
    Zarr(Vec<ZarrType>),
}

pub trait Ops {
    fn get_subject(&self, subject: &str) -> OpsResult;
    fn get_predicate(&self, predicate: &str) -> OpsResult;
    fn get_object(&self, object: &str) -> OpsResult;
}

impl<R: ReadableStorageTraits, T: TriviallyTransmutable, C> Ops for Storage<R, T, C> {
    fn get_subject(&self, subject: &str) -> OpsResult {
        let index = match self.dictionary.get_subject_idx(subject) {
            Some(index) => index,
            None => return Err(OpsError::SubjectNotFound),
        };

        let ans = match self.serialization {
            Serialization::Zarr => match &self.array {
                Some(array) => OpsFormat::Zarr(match self.reference_system {
                    ReferenceSystem::SPO | ReferenceSystem::SOP => array.get_first_term(index)?,
                    ReferenceSystem::PSO | ReferenceSystem::OSP => array.get_second_term(index)?,
                    ReferenceSystem::POS | ReferenceSystem::OPS => array.get_third_term(index)?,
                }),
                None => return Err(OpsError::EmptyArray),
            },
            Serialization::Sparse => match &self.sparse_array {
                Some(array) => OpsFormat::SparseArray(match self.reference_system {
                    ReferenceSystem::SPO | ReferenceSystem::SOP => array.get_first_term(index)?,
                    ReferenceSystem::PSO | ReferenceSystem::OSP => array.get_second_term(index)?,
                    ReferenceSystem::POS | ReferenceSystem::OPS => array.get_third_term(index)?,
                }),
                None => return Err(OpsError::EmptySparseArray),
            },
        };

        Ok(ans)
    }

    fn get_predicate(&self, predicate: &str) -> OpsResult {
        let index = match self.dictionary.get_predicate_idx(predicate) {
            Some(index) => index,
            None => return Err(OpsError::PredicateNotFound),
        };

        let ans = match self.serialization {
            Serialization::Zarr => match &self.array {
                Some(array) => OpsFormat::Zarr(match self.reference_system {
                    ReferenceSystem::PSO | ReferenceSystem::POS => array.get_first_term(index)?,
                    ReferenceSystem::SPO | ReferenceSystem::OPS => array.get_second_term(index)?,
                    ReferenceSystem::SOP | ReferenceSystem::OSP => array.get_third_term(index)?,
                }),
                None => return Err(OpsError::EmptyArray),
            },
            Serialization::Sparse => match &self.sparse_array {
                Some(array) => OpsFormat::SparseArray(match self.reference_system {
                    ReferenceSystem::PSO | ReferenceSystem::POS => array.get_first_term(index)?,
                    ReferenceSystem::SPO | ReferenceSystem::OPS => array.get_second_term(index)?,
                    ReferenceSystem::SOP | ReferenceSystem::OSP => array.get_third_term(index)?,
                }),
                None => return Err(OpsError::EmptySparseArray),
            },
        };

        Ok(ans)
    }

    fn get_object(&self, object: &str) -> OpsResult {
        let index = match self.dictionary.get_object_idx(object) {
            Some(index) => index,
            None => return Err(OpsError::ObjectNotFound),
        };

        let ans = match self.serialization {
            Serialization::Zarr => match &self.array {
                Some(array) => OpsFormat::Zarr(match self.reference_system {
                    ReferenceSystem::OPS | ReferenceSystem::OSP => array.get_first_term(index)?,
                    ReferenceSystem::SOP | ReferenceSystem::POS => array.get_second_term(index)?,
                    ReferenceSystem::SPO | ReferenceSystem::PSO => array.get_third_term(index)?,
                }),
                None => return Err(OpsError::EmptyArray),
            },
            Serialization::Sparse => match &self.sparse_array {
                Some(array) => OpsFormat::SparseArray(match self.reference_system {
                    ReferenceSystem::OPS | ReferenceSystem::OSP => array.get_first_term(index)?,
                    ReferenceSystem::SOP | ReferenceSystem::POS => array.get_second_term(index)?,
                    ReferenceSystem::SPO | ReferenceSystem::PSO => array.get_third_term(index)?,
                }),
                None => return Err(OpsError::EmptySparseArray),
            },
        };

        Ok(ans)
    }
}
