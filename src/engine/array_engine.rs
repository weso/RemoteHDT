use nalgebra_sparse::ops::serial::{spmm_csr_pattern, spmm_csr_prealloc};
use nalgebra_sparse::ops::Op::NoOp;
use nalgebra_sparse::{CooMatrix, CsrMatrix};

use crate::remote_hdt::ZarrArray;

use super::{EngineResult, EngineStrategy};

impl EngineStrategy for ZarrArray {
    fn get_subject(&self, indices: Vec<usize>) -> EngineResult {
        let mut b_mat = CooMatrix::<u32>::zeros(self.nrows(), self.ncols());
        indices
            .iter()
            .for_each(|&index| b_mat.push(index, index, 1));
        let selection = CsrMatrix::<u32>::from(&b_mat);
        let pattern = spmm_csr_pattern(selection.pattern(), self.pattern());
        let nnz = pattern.nnz();
        let mut ans =
            CsrMatrix::<u32>::try_from_pattern_and_values(pattern, vec![0u32; nnz]).unwrap();
        spmm_csr_prealloc(0, &mut ans, 1, NoOp(&selection), NoOp(self)).unwrap();
        Ok(ans)
    }

    fn get_predicate(&self, index: usize) -> EngineResult {
        unimplemented!()
    }

    fn get_object(&self, indices: Vec<usize>) -> EngineResult {
        unimplemented!()
    }
}
