use std::collections::HashSet;

use fcsd::Set;

use super::utils::hash_to_set;

#[derive(Clone)]
pub struct Dictionary {
    subjects: Set,
    predicates: Set,
    objects: Set,
}

impl Default for Dictionary {
    fn default() -> Self {
        Dictionary {
            subjects: Set::new(vec!["PlaceHolder"]).unwrap(),
            predicates: Set::new(vec!["PlaceHolder"]).unwrap(),
            objects: Set::new(vec!["PlaceHolder"]).unwrap(),
        }
    }
}

impl Dictionary {
    pub(crate) fn from_vec_str(
        subjects: &Vec<String>,
        predicates: &Vec<String>,
        objects: &Vec<String>,
    ) -> Self {
        Dictionary {
            subjects: Set::new(subjects).unwrap(),
            predicates: Set::new(predicates).unwrap(),
            objects: Set::new(objects).unwrap(),
        }
    }

    pub(crate) fn from_set_terms(
        subjects: HashSet<String>,
        predicates: HashSet<String>,
        objects: HashSet<String>,
    ) -> Self {
        Dictionary {
            subjects: Set::new(hash_to_set(subjects)).unwrap(),
            predicates: Set::new(hash_to_set(predicates)).unwrap(),
            objects: Set::new(hash_to_set(objects)).unwrap(),
        }
    }

    pub fn subjects_size(&self) -> usize {
        self.subjects.len()
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

    pub fn get_subject_idx(&self, subject: &str) -> Option<usize> {
        let mut locator = self.subjects.locator();
        locator.run(subject)
    }

    pub fn get_subject_idx_unchecked(&self, subject: &str) -> usize {
        self.get_subject_idx(subject).unwrap()
    }

    pub fn get_predicate_idx(&self, predicate: &str) -> Option<usize> {
        let mut locator = self.predicates.locator();
        locator.run(predicate).map(|value| value + 1)
    }

    pub fn get_predicate_idx_unchecked(&self, predicate: &str) -> usize {
        self.get_predicate_idx(predicate).unwrap()
    }

    pub fn get_object_idx(&self, object: &str) -> Option<usize> {
        let mut locator = self.objects.locator();
        locator.run(object)
    }

    pub fn get_object_idx_unchecked(&self, object: &str) -> usize {
        self.get_object_idx(object).unwrap()
    }
}
