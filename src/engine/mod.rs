use crate::error::EngineError;

pub mod array;
pub mod chunk;

pub type EngineResult<T> = Result<T, EngineError>;

pub trait EngineStrategy<T> {
    fn get_subject(&self, index: usize) -> EngineResult<T>;
    fn get_predicate(&self, index: u8) -> EngineResult<T>;
    fn get_object(&self, index: usize) -> EngineResult<T>;
}
