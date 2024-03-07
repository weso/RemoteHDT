use fcsd::Set;
use std::collections::HashSet;

use crate::storage::params::ReferenceSystem;

use super::utils::hash_to_set;

#[derive(Clone)]
pub struct Dictionary {
    reference_system: ReferenceSystem,
    subjects: Set,
    predicates: Set,
    objects: Set,
}

impl Default for Dictionary {
    fn default() -> Self {
        Dictionary {
            reference_system: ReferenceSystem::SPO,
            subjects: Set::new(vec!["PlaceHolder"]).unwrap(),
            predicates: Set::new(vec!["PlaceHolder"]).unwrap(),
            objects: Set::new(vec!["PlaceHolder"]).unwrap(),
        }
    }
}

impl Dictionary {
    pub(crate) fn from_vec_str(
        reference_system: ReferenceSystem,
        subjects: &Vec<String>,
        predicates: &Vec<String>,
        objects: &Vec<String>,
    ) -> Self {
        Dictionary {
            reference_system,
            subjects: Set::new(subjects).unwrap(),
            predicates: Set::new(predicates).unwrap(),
            objects: Set::new(objects).unwrap(),
        }
    }

    pub(crate) fn from_set_terms(
        reference_system: ReferenceSystem,
        subjects: HashSet<String>,
        predicates: HashSet<String>,
        objects: HashSet<String>,
    ) -> Self {
        Dictionary {
            reference_system,
            subjects: Set::new(hash_to_set(subjects)).unwrap(),
            predicates: Set::new(hash_to_set(predicates)).unwrap(),
            objects: Set::new(hash_to_set(objects)).unwrap(),
        }
        
    }

    pub fn subjects_size(&self) -> usize {
        self.subjects.len()
    }

    pub fn predicates_size(&self) -> usize {
        self.predicates.len()
    }

    pub fn objects_size(&self) -> usize {
        self.objects.len()
    }

    pub fn subjects(&self) -> Set {
        self.subjects.to_owned()
    }

    pub fn predicates(&self) -> Set {
        self.predicates.to_owned()
    }

    pub fn objects(&self) -> Set {
        self.objects.to_owned()
    }

    pub fn get_reference_system(&self) -> ReferenceSystem {
        self.reference_system.to_owned()
    }

    pub fn get_subject_idx(&self, subject: &str) -> Option<usize> {
        let mut locator = self.subjects.locator();
        match self.reference_system {
            ReferenceSystem::PSO | ReferenceSystem::OSP => {
                locator.run(subject).map(|value| value + 1)
            }
            _ => locator.run(subject),
        }
    }

    pub fn get_subject_idx_unchecked(&self, subject: &str) -> usize {
        self.get_subject_idx(subject).unwrap()
    }

    pub fn get_predicate_idx(&self, predicate: &str) -> Option<usize> {
        let mut locator = self.predicates.locator();
        match self.reference_system {
            ReferenceSystem::SPO | ReferenceSystem::OPS => {
                locator.run(predicate).map(|value| value + 1)
            }
            _ => locator.run(predicate),
        }
    }

    pub fn get_predicate_idx_unchecked(&self, predicate: &str) -> usize {
        self.get_predicate_idx(predicate).unwrap()
    }

    pub fn get_object_idx(&self, object: &str) -> Option<usize> {
        let mut locator = self.objects.locator();
        match self.reference_system {
            ReferenceSystem::SOP | ReferenceSystem::POS => {
                locator.run(object).map(|value| value + 1)
            }
            _ => locator.run(object),
        }
    }

    pub fn get_object_idx_unchecked(&self, object: &str) -> usize {
        self.get_object_idx(object).unwrap()
    }
}
