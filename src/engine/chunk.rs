use zarrs::array_subset::ArraySubset;
use zarrs::{array::Array, storage::ReadableStorageTraits};

use crate::error::EngineError::OperationError;
use crate::utils::{objects_per_chunk, subjects_per_chunk};

use super::{EngineResult, EngineStrategy};

impl<T: ReadableStorageTraits> EngineStrategy<Vec<u8>> for Array<T> {
    fn get_subject(&self, index: usize) -> EngineResult<Vec<u8>> {
        let index_to_chunk = index as u64 / subjects_per_chunk(self);
        let chunk_to_index = index % subjects_per_chunk(self) as usize;
        match self
            .retrieve_chunk(&[index_to_chunk, 0])?
            .chunks(objects_per_chunk(self) as usize)
            .nth(chunk_to_index)
        {
            Some(ans) => Ok(ans.to_owned()),
            None => Err(OperationError),
        }
    }

    fn get_predicate(&self, index: usize) -> EngineResult<Vec<u8>> {
        unimplemented!()
    }

    fn get_object(&self, index: usize) -> EngineResult<Vec<u8>> {
        let start = vec![0, index as u64];
        let end = vec![self.shape()[0], index as u64];
        let shape = &ArraySubset::new_with_start_end_inc(start, end)?;
        let ans = self.retrieve_array_subset_elements(shape)?;
        Ok(ans)
    }
}
