use std::error::Error;

use ndarray::{ArcArray, ArcArray1, Array2, Axis, IxDyn};

use super::EngineStrategy;

impl EngineStrategy for ArcArray<u8, IxDyn> {
    fn get_subject(&self, index: usize) -> Result<ArcArray<u64, IxDyn>, Box<dyn Error>> {
        unimplemented!()
    }

    // TODO: the current implementation works for SELECT *, but what if we SELECT predicate?
    fn get_predicate(&self, index: usize) -> Result<ArcArray<u64, IxDyn>, Box<dyn Error>> {
        unimplemented!()
    }

    fn get_object(&self, index: usize) -> Result<ArcArray<u64, IxDyn>, Box<dyn Error>> {
        unimplemented!()
    }
}
