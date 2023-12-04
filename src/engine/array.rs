use sprs::{CsMat, TriMat};

use crate::storage::ZarrArray;

use super::{EngineResult, EngineStrategy};

impl EngineStrategy<CsMat<u8>> for ZarrArray {
    fn get_subject(&self, index: usize) -> EngineResult<CsMat<u8>> {
        let mut matrix = TriMat::new((self.rows(), self.rows()));
        matrix.add_triplet(index, index, 1);
        let matrix = matrix.to_csc();
        Ok(&matrix * self)
    }

    fn get_predicate(&self, value: u8) ->  EngineResult<CsMat<u8>>  {
        
        unimplemented!()
    }

    fn get_object(&self, index: usize) -> EngineResult<CsMat<u8>> {     
        let mut matrix = TriMat::new((self.cols(), self.cols()));
        matrix.add_triplet(index, index, 1);
        let matrix = matrix.to_csc();
        Ok(self * &matrix)
    }
}
