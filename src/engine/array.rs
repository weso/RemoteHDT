use sprs::{CsVec, TriMat};

use crate::storage::ZarrArray;

use super::{EngineResult, EngineStrategy};

impl EngineStrategy<CsVec<u8>> for ZarrArray {
    fn get_subject(&self, index: usize) -> EngineResult<CsVec<u8>> {
        let selection = CsVec::new(self.rows(), vec![index], vec![1]);
        Ok(&self.transpose_view() * &selection)
    }

    fn get_predicate(&self, index: usize) -> EngineResult<CsVec<u8>> {
        unimplemented!()
    }

    fn get_object(&self, indices: Vec<usize>) -> EngineResult<CsVec<u8>> {
        unimplemented!()
    }
}
