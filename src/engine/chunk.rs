use zarrs::array::Array;
use zarrs::array_subset::ArraySubset;
use zarrs::storage::ReadableStorageTraits;

use crate::utils::columns_per_shard;
use crate::utils::rows_per_shard;

use super::EngineResult;
use super::EngineStrategy;

impl<T: ReadableStorageTraits + 'static> EngineStrategy<Vec<u32>> for Array<T> {
    fn get_first_term(&self, index: usize) -> EngineResult<Vec<u32>> {
        let shard_index = index as u64 / rows_per_shard(self);
        let shard = self.retrieve_chunk_elements(&[shard_index, 0])?;
        let chunk_index = index as u64 % rows_per_shard(self);
        let start = (chunk_index * columns_per_shard(self)) as usize;
        let end = start + columns_per_shard(self) as usize;
        let chunk: &[u32] = &shard[start..end];
        Ok(chunk.to_vec())
    }

    fn get_second_term(&self, _index: usize) -> EngineResult<Vec<u32>> {
        unimplemented!()
    }

    fn get_third_term(&self, index: usize) -> EngineResult<Vec<u32>> {
        let col = index as u64;
        let shape = ArraySubset::new_with_start_end_inc(vec![0, col], vec![self.shape()[0], col])?;
        let ans = self.retrieve_array_subset_elements(&shape)?;
        Ok(ans)
    }
}
