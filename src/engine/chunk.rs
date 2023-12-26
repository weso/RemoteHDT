use zarrs::array::Array;
use zarrs::array_subset::ArraySubset;
use zarrs::storage::ReadableStorageTraits;

use crate::storage::ZarrType;
use crate::utils::columns_per_shard;
use crate::utils::rows_per_shard;

use super::EngineResult;
use super::EngineStrategy;

impl<T: ReadableStorageTraits> EngineStrategy<Vec<ZarrType>> for Array<T> {
    fn get_first_term(&self, index: usize) -> EngineResult<Vec<ZarrType>> {
        let index_to_chunk = index as u64 / rows_per_shard(self);
        let chunk_to_index = index as u64 % rows_per_shard(self);
        Ok(self
            .retrieve_chunk_subset_elements(
                &[index_to_chunk, 0],
                &ArraySubset::new_with_start_end_inc(
                    vec![chunk_to_index, 0],
                    vec![chunk_to_index, columns_per_shard(self) - 1],
                )?,
            )?
            .to_vec())
    }

    fn get_second_term(&self, _index: usize) -> EngineResult<Vec<ZarrType>> {
        unimplemented!()
    }

    fn get_third_term(&self, index: usize) -> EngineResult<Vec<ZarrType>> {
        let start = vec![0, index as u64];
        let end = vec![self.shape()[0], index as u64];
        let shape = &ArraySubset::new_with_start_end_inc(start, end)?;
        let ans = self.retrieve_array_subset_elements(shape)?;
        Ok(ans.to_vec())
    }
}
