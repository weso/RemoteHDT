use std::error::Error;

use ndarray::{ArcArray, IxDyn};
use zarrs::{array::Array, array_subset::ArraySubset, storage::ReadableWritableStorageTraits};

use crate::{reference_system::ReferenceSystem, remote_hdt::Field};

use super::EngineStrategy;

pub struct ChunkEngine;

impl ChunkEngine {
    fn get(
        array: &Array<dyn ReadableWritableStorageTraits>,
        term: Field,
        reference_system: ReferenceSystem,
    ) -> Result<ArcArray<u8, IxDyn>, Box<dyn Error>> {
        match term {
            Field::Subject(term) => match reference_system {
                ReferenceSystem::SPO | ReferenceSystem::SOP => Self::get_first_term(array, term),
                ReferenceSystem::PSO | ReferenceSystem::OSP => Self::get_second_term(array, term),
                ReferenceSystem::POS | ReferenceSystem::OPS => Self::get_third_term(array, term),
            },
            Field::Predicate(term) => match reference_system {
                ReferenceSystem::PSO | ReferenceSystem::POS => Self::get_first_term(array, term),
                ReferenceSystem::SPO | ReferenceSystem::OPS => Self::get_second_term(array, term),
                ReferenceSystem::SOP | ReferenceSystem::OSP => Self::get_third_term(array, term),
            },
            Field::Object(term) => match reference_system {
                ReferenceSystem::OPS | ReferenceSystem::OSP => Self::get_first_term(array, term),
                ReferenceSystem::POS | ReferenceSystem::SOP => Self::get_second_term(array, term),
                ReferenceSystem::SPO | ReferenceSystem::PSO => Self::get_third_term(array, term),
            },
        }
    }

    fn get_first_term(
        array: &Array<dyn ReadableWritableStorageTraits>,
        term: usize,
    ) -> Result<ArcArray<u8, IxDyn>, Box<dyn Error>> {
        let shape = ArraySubset::new_with_start_end_inc(
            vec![term as u64, 0, 0],
            vec![term as u64, array.shape()[1], array.shape()[2]],
        )?;

        Ok(array.par_retrieve_array_subset_ndarray(&shape)?.into())
    }

    fn get_second_term(
        array: &Array<dyn ReadableWritableStorageTraits>,
        term: usize,
    ) -> Result<ArcArray<u8, IxDyn>, Box<dyn Error>> {
        let shape = ArraySubset::new_with_start_end_inc(
            vec![0, term as u64, 0],
            vec![array.shape()[0], term as u64, array.shape()[2]],
        )?;

        Ok(array.par_retrieve_array_subset_ndarray(&shape)?.into())
    }

    fn get_third_term(
        array: &Array<dyn ReadableWritableStorageTraits>,
        term: usize,
    ) -> Result<ArcArray<u8, IxDyn>, Box<dyn Error>> {
        let shape = ArraySubset::new_with_start_end_inc(
            vec![0, 0, term as u64],
            vec![array.shape()[0], array.shape()[1], term as u64],
        )?;

        Ok(array.par_retrieve_array_subset_ndarray(&shape)?.into())
    }
}

impl<'a> EngineStrategy for Array<dyn ReadableWritableStorageTraits> {
    fn get_subject(
        &self,
        index: usize,
        reference_system: ReferenceSystem,
    ) -> Result<ArcArray<u8, IxDyn>, Box<dyn Error>> {
        ChunkEngine::get(self, Field::Subject(index), reference_system)
    }

    // TODO: the current implementation works for SELECT *, but what if we SELECT predicate?
    fn get_predicate(
        &self,
        index: usize,
        reference_system: ReferenceSystem,
    ) -> Result<ArcArray<u8, IxDyn>, Box<dyn Error>> {
        ChunkEngine::get(self, Field::Predicate(index), reference_system)
    }

    fn get_object(
        &self,
        index: usize,
        reference_system: ReferenceSystem,
    ) -> Result<ArcArray<u8, IxDyn>, Box<dyn Error>> {
        ChunkEngine::get(self, Field::Object(index), reference_system)
    }
}
