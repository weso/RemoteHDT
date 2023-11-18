use sprs::CsVec;
use zarrs::{array::Array, storage::ReadableStorageTraits};

use super::{EngineResult, EngineStrategy};

impl<T: ReadableStorageTraits> EngineStrategy<Vec<u8>> for Array<T> {
    fn get_subject(&self, index: usize) -> EngineResult<Vec<u8>> {
        let ans = self.retrieve_chunk(&[index as u64, 0])?;
        Ok(ans)
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
