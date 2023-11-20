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

    // TODO: the current implementation works for SELECT *, but what if we SELECT predicate?
    fn get_predicate(&self, index: usize) -> EngineResult<Vec<u8>> {
        unimplemented!()
    }

    fn get_object(&self, index: usize) -> EngineResult<Vec<u8>> {
        let ans = self.retrieve_chunk(&[0, index as u64])?;
        Ok(ans)
    }
}
