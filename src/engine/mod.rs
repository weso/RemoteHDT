use std::error::Error;

use ndarray::{ArcArray, IxDyn};

use crate::reference_system::ReferenceSystem;

pub mod array_engine;
pub mod chunk_engine;

pub trait EngineStrategy {
    fn get_subject(
        &self,
        index: usize,
        reference_system: ReferenceSystem, // TODO: this is not needed, fix !!!!
    ) -> Result<ArcArray<u8, IxDyn>, Box<dyn Error>>;

    fn get_predicate(
        &self,
        index: usize,
        reference_system: ReferenceSystem,
    ) -> Result<ArcArray<u8, IxDyn>, Box<dyn Error>>;

    fn get_object(
        &self,
        index: usize,
        reference_system: ReferenceSystem,
    ) -> Result<ArcArray<u8, IxDyn>, Box<dyn Error>>;
}
