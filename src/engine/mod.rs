use crate::error::EngineError;

pub mod array;
pub mod chunk;

pub(crate) type EngineResult<T> = Result<T, EngineError>;

pub(crate) trait EngineStrategy<T> {
    fn get_first_term(&self, index: usize) -> EngineResult<T>;
    fn get_second_term(&self, index: usize) -> EngineResult<T>;
    fn get_third_term(&self, index: usize) -> EngineResult<T>;
}
