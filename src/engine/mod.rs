use crate::{error::EngineError, remote_hdt::ZarrArray};

pub mod array_engine;

pub type EngineResult = Result<ZarrArray, EngineError>;

pub trait EngineStrategy {
    fn get_subject(&self, indices: Vec<usize>) -> EngineResult;
    fn get_predicate(&self, indices: Vec<usize>) -> EngineResult;
    fn get_object(&self, indices: usize) -> EngineResult;
}
