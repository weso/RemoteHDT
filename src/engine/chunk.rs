use zarrs::array::Array;
use zarrs::array_subset::ArraySubset;
use zarrs::storage::ReadableStorageTraits;

use crate::error::EngineError;
use crate::utils::columns_per_shard;
use crate::utils::rows_per_shard;

use super::EngineResult;
use super::EngineStrategy;

impl<T: ReadableStorageTraits + 'static + ?Sized> EngineStrategy<Vec<u32>> for Array<T> {
    fn get_first_term(&self, index: usize) -> EngineResult<Vec<u32>> {
        let shard_index = index as u64 / rows_per_shard(self);
        let shard = self.retrieve_chunk_elements(&[shard_index, 0])?;
        let chunk_index = index as u64 % rows_per_shard(self);
        let start = (chunk_index * columns_per_shard(self)) as usize;
        let end = start + columns_per_shard(self) as usize;
        let chunk: &[u32] = &shard[start..end];
        Ok(chunk.to_vec())
    }

    fn get_second_term(&self, index: usize) -> EngineResult<Vec<u32>> {
        let mut ans = Vec::new();
        let number_of_shards = match self.chunk_grid_shape() {
            Some(chunk_grid) => chunk_grid[0],
            None => return Err(EngineError::Operation),
        };
        for i in 0..number_of_shards {
            let mut shard = self.retrieve_chunk_elements::<u32>(&[i, 0])?;
            shard.iter_mut().for_each(|e| {
                if *e != index as u32 {
                    *e = 0
                }
            });
            ans.append(&mut shard);
        }
        Ok(ans)
    }

    fn get_third_term(&self, index: usize) -> EngineResult<Vec<u32>> {
        let objects = self.shape()[0];
        let col = index as u64;
        let shape = ArraySubset::new_with_ranges(&[0..objects, col..col + 1]);
        let array_subset = self.retrieve_array_subset_elements::<u32>(&shape)?;
        Ok(array_subset)
    }
}
