use sprs::{CsVec, TriMat};

use crate::storage::ZarrArray;

use super::{EngineResult, EngineStrategy};

impl EngineStrategy for ZarrArray {
    fn get_subject(&self, indices: Vec<usize>) -> EngineResult {
        let mut selection = TriMat::new((self.rows(), self.rows()));
        indices
            .iter()
            .for_each(|&index| selection.add_triplet(index, index, 1));
        Ok(&selection.to_csc() * self)
    }

    fn get_predicate(&self, index: usize) -> EngineResult {
        unimplemented!()
    }

    fn get_object(&self, indices: Vec<usize>) -> EngineResult {
        unimplemented!()
    }

    fn get_neighborhood(&self, index: usize) -> CsVec<u8> {
        let selection = CsVec::new(self.rows(), vec![index], vec![1]);
        &self.transpose_view() * &selection
    }
}
