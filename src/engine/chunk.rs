use zarrs::array::Array;
use zarrs::array_subset::ArraySubset;
use zarrs::storage::ReadableStorageTraits;

use crate::utils::columns_per_shard;
use crate::utils::rows_per_shard;

use super::EngineResult;
use super::EngineStrategy;

impl<T: ReadableStorageTraits> EngineStrategy<Vec<usize>> for Array<T> {
    fn get_first_term(&self, index: usize) -> EngineResult<Vec<usize>> {
        let index_to_chunk = index as u64 / rows_per_shard(self);
        let chunk_to_index = index as u64 % rows_per_shard(self);
        let ans = self.retrieve_chunk_subset_elements(
            &[index_to_chunk, 0],
            &ArraySubset::new_with_ranges(&[
                chunk_to_index..chunk_to_index + 1,
                0..columns_per_shard(self),
            ]),
        )?;
        Ok(ans.to_vec())
    }

    fn get_second_term(&self, _index: usize) -> EngineResult<Vec<usize>> {
        unimplemented!()
    }

    fn get_third_term(&self, index: usize) -> EngineResult<Vec<usize>> {
        let last_chunk = self.shape()[0] / rows_per_shard(self);
        let col = index as u64;
        let shape = &ArraySubset::new_with_ranges(&[0..last_chunk, col..col + 1]);
        let ans = self.retrieve_array_subset_elements(shape)?;
        Ok(ans.to_vec())
    }
}
