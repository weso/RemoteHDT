use sprs::{CsVec, CsMat, CsMatBase};

use crate::storage::ZarrArray;

use super::{EngineResult, EngineStrategy};

impl EngineStrategy<CsVec<u8>> for ZarrArray {
    fn get_subject(&self, index: usize) -> EngineResult<CsVec<u8>> {
        let selection = CsVec::new(self.rows(), vec![index], vec![1]);
        Ok(&self.transpose_view() * &selection)
    }

    fn get_predicate(&self, index: usize) -> EngineResult<CsMat<u8>> {

        
        let mut rows: Vec<usize> = vec![];
        let mut cols: Vec<usize> = vec![];
        let mut values: Vec<u8> = vec![];
        let iterator = self.into_iter();
        for value in iterator {
            if *value.0 == index as u8 {
                rows.push(value.1.0);
                cols.push(value.1.1);
                values.push(*value.0);
            }
           
        }
        //CsMatBase<u8, usize, Vec<usize>, Vec<usize>, Vec<u8>>
        let result = CsMat::new(self.shape(),rows, cols, values);
        
        Ok(result)
    }

    fn get_object(&self, index: usize) -> EngineResult<CsVec<u8>> {
        let selection = CsVec::new(self.cols(), vec![index], vec![1]);
        Ok(self * &selection)
    }
}
