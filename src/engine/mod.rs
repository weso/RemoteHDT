use std::error::Error;

use ndarray::{ArcArray, IxDyn};

pub mod array_engine;
pub mod chunk_engine;

pub trait EngineStrategy {
    fn get_subject(&self, index: usize) -> Result<ArcArray<u64, IxDyn>, Box<dyn Error>>;
    fn get_predicate(&self, index: usize) -> Result<ArcArray<u64, IxDyn>, Box<dyn Error>>;
    fn get_object(&self, index: usize) -> Result<ArcArray<u64, IxDyn>, Box<dyn Error>>;
}
