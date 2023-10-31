use std::error::Error;

use ndarray::{ArcArray, ArcArray1, Array2, Axis, Ix3, IxDyn};

use crate::{reference_system::ReferenceSystem, remote_hdt::Field};

use super::EngineStrategy;

pub struct ArrayEngine;

impl ArrayEngine {
    fn get(
        array: &ArcArray<u8, IxDyn>,
        term: Field,
        reference_system: ReferenceSystem,
    ) -> Result<ArcArray<u8, IxDyn>, Box<dyn Error>> {
        let arr = array.clone().into_dimensionality::<Ix3>()?;
        let binding = array.clone();
        let shape = binding.shape();

        let flattened: ArcArray1<u8> = match term {
            Field::Subject(term) => match reference_system {
                ReferenceSystem::SPO | ReferenceSystem::SOP => {
                    Self::get_first_term(arr, shape[0], term)
                }
                ReferenceSystem::PSO | ReferenceSystem::OSP => {
                    Self::get_second_term(arr, shape[1], term)
                }
                ReferenceSystem::POS | ReferenceSystem::OPS => {
                    Self::get_third_term(arr, shape[2], term)
                }
            },
            Field::Predicate(term) => match reference_system {
                ReferenceSystem::SPO | ReferenceSystem::OPS => {
                    Self::get_second_term(arr, shape[1], term)
                }
                ReferenceSystem::SOP | ReferenceSystem::OSP => {
                    Self::get_third_term(arr, shape[2], term)
                }
                ReferenceSystem::PSO | ReferenceSystem::POS => {
                    Self::get_first_term(arr, shape[0], term)
                }
            },
            Field::Object(term) => match reference_system {
                ReferenceSystem::SPO | ReferenceSystem::PSO => {
                    Self::get_third_term(arr, shape[2], term)
                }
                ReferenceSystem::SOP | ReferenceSystem::POS => {
                    Self::get_second_term(arr, shape[1], term)
                }
                ReferenceSystem::OSP | ReferenceSystem::OPS => {
                    Self::get_first_term(arr, shape[0], term)
                }
            },
        };

        Ok(flattened.into_shape(shape)?)
    }

    fn get_first_term(array: ArcArray<u8, Ix3>, size: usize, term: usize) -> ArcArray1<u8> {
        array
            .axis_iter(Axis(0))
            .enumerate()
            .flat_map(|(i, two_dim_array)| {
                let factor: Array2<u8> = if i == term {
                    Array2::eye(size)
                } else {
                    Array2::zeros((size, size))
                };
                factor.dot(&two_dim_array)
            })
            .collect::<ArcArray1<u8>>()
    }

    fn get_second_term(array: ArcArray<u8, Ix3>, size: usize, term: usize) -> ArcArray1<u8> {
        let mut factor: Array2<u8> = Array2::zeros((size, size));
        factor[[term, term]] = 1; // we place it in the main diagonal

        array
            .axis_iter(Axis(0))
            .flat_map(|two_dim_array| factor.dot(&two_dim_array))
            .collect::<ArcArray1<u8>>()
    }

    fn get_third_term(array: ArcArray<u8, Ix3>, size: usize, term: usize) -> ArcArray1<u8> {
        let mut factor: Array2<u8> = Array2::zeros((size, size));
        factor[[term, term]] = 1; // we place it in the main diagonal

        array
            .axis_iter(Axis(0))
            .flat_map(|two_dim_array| two_dim_array.dot(&factor))
            .collect::<ArcArray1<u8>>()
    }
}

impl EngineStrategy for ArcArray<u8, IxDyn> {
    fn get_subject(
        &self,
        index: usize,
        reference_system: ReferenceSystem,
    ) -> Result<ArcArray<u8, IxDyn>, Box<dyn Error>> {
        ArrayEngine::get(self, Field::Subject(index), reference_system)
    }

    // TODO: the current implementation works for SELECT *, but what if we SELECT predicate?
    fn get_predicate(
        &self,
        index: usize,
        reference_system: ReferenceSystem,
    ) -> Result<ArcArray<u8, IxDyn>, Box<dyn Error>> {
        ArrayEngine::get(self, Field::Predicate(index), reference_system)
    }

    fn get_object(
        &self,
        index: usize,
        reference_system: ReferenceSystem,
    ) -> Result<ArcArray<u8, IxDyn>, Box<dyn Error>> {
        ArrayEngine::get(self, Field::Object(index), reference_system)
    }
}
