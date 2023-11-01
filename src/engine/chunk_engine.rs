use std::error::Error;

use ndarray::{ArcArray, IxDyn};
use zarrs::{array::Array, array_subset::ArraySubset, storage::ReadableWritableStorageTraits};

use super::EngineStrategy;

impl<'a> EngineStrategy for Array<dyn ReadableWritableStorageTraits> {
    fn get_subject(&self, index: usize) -> Result<ArcArray<u64, IxDyn>, Box<dyn Error>> {
        Ok(self
            .par_retrieve_array_subset_ndarray(&ArraySubset::new_with_start_end_inc(
                vec![index as u64, 0],
                vec![index as u64, self.shape()[1]],
            )?)?
            .into()) // TODO: Improve Errors here :D
    }

    // TODO: the current implementation works for SELECT *, but what if we SELECT predicate?
    fn get_predicate(&self, index: usize) -> Result<ArcArray<u64, IxDyn>, Box<dyn Error>> {
        Ok(self
            .par_retrieve_array_subset_ndarray(&ArraySubset::new_with_start_end_inc(
                vec![0, index as u64],
                vec![self.shape()[0], index as u64],
            )?)?
            .into()) // TODO: Improve Errors here :D
    }

    fn get_object(&self, index: usize) -> Result<ArcArray<u64, IxDyn>, Box<dyn Error>> {
        unimplemented!()
    }
}
