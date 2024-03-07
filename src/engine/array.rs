use sprs::TriMat;

use crate::storage::ZarrArray;

use super::EngineResult;
use super::EngineStrategy;

impl EngineStrategy<ZarrArray> for ZarrArray {
    fn get_first_term(&self, index: usize) -> EngineResult<ZarrArray> {
        let mut matrix = TriMat::new((self.rows(), self.rows()));
        matrix.add_triplet(index, index, 1);
        let matrix = matrix.to_csc();
        Ok(&matrix * self)
    }

    fn get_second_term(&self, value: usize) -> EngineResult<ZarrArray> {
        let mut matrix = TriMat::new((self.rows(), self.cols()));
        self.iter().for_each(|(&e, (row, col))| {
            if e == value {
                matrix.add_triplet(row, col, value);
            }
        });
        Ok(matrix.to_csc())
    }

    fn get_third_term(&self, index: usize) -> EngineResult<ZarrArray> {
        let mut matrix = TriMat::new((self.cols(), self.cols()));
        matrix.add_triplet(index, index, 1);
        let matrix = matrix.to_csc();
        Ok(self * &matrix)
    }
}
