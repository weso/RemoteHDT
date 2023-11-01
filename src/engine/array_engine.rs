use nalgebra_sparse::ops::serial::spmm_csr_prealloc;
use nalgebra_sparse::ops::Op::NoOp;
use nalgebra_sparse::{CooMatrix, CsrMatrix};

use crate::remote_hdt::ZarrArray;

use super::{EngineResult, EngineStrategy};

impl EngineStrategy for ZarrArray {
    fn get_subject(&self, indices: Vec<usize>) -> EngineResult {
        let mut selection = CooMatrix::zeros(self.ncols(), self.nrows());
        indices
            .iter()
            .for_each(|&index| selection.push(index, index, 1));
        let mut ans = ZarrArray::zeros(self.ncols(), self.ncols());
        let _ = spmm_csr_prealloc(
            1,
            &mut ans,
            1,
            NoOp(&CsrMatrix::from(&selection)),
            NoOp(&self),
        );
        Ok(ans)
    }

    fn get_predicate(&self, indices: Vec<usize>) -> EngineResult {
        let mut selection = CooMatrix::zeros(self.ncols(), self.nrows());
        indices
            .iter()
            .for_each(|&index| selection.push(index, index, 1));
        let mut ans = ZarrArray::zeros(self.nrows(), self.nrows());
        let _ = spmm_csr_prealloc(
            1,
            &mut ans,
            1,
            NoOp(&self),
            NoOp(&CsrMatrix::from(&selection)),
        );
        Ok(ans)
    }

    fn get_object(&self, index: usize) -> EngineResult {
        unimplemented!()
    }
}
