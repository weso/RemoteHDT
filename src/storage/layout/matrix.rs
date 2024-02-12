use parking_lot::Mutex;
use sprs::TriMat;
use std::num::NonZeroU64;
use std::sync::atomic::Ordering;
use zarrs::array::codec::array_to_bytes::sharding::ShardingCodecBuilder;
use zarrs::array::codec::ArrayToBytesCodecTraits;
use zarrs::array::codec::GzipCodec;
use zarrs::array::ChunkGrid;
use zarrs::array::DataType;
use zarrs::array::DimensionName;
use zarrs::array::FillValue;

use super::ChunkingStrategy;
use super::Dimensionality;
use super::ReferenceSystem;
use super::StorageResult;

use crate::io::Graph;
use crate::storage::layout::LayoutOps;
use crate::storage::AtomicZarrType;
use crate::storage::Layout;

type Chunk = Vec<(u32, u32)>;

pub struct MatrixLayout;

impl Layout<Chunk> for MatrixLayout {
    fn shape(&self, dimensionality: &Dimensionality) -> Vec<u64> {
        vec![
            dimensionality.get_first_term_size(),
            dimensionality.get_third_term_size(),
        ]
    }

    fn data_type(&self) -> DataType {
        DataType::UInt64
    }

    fn chunk_shape(
        &self,
        chunking_strategy: ChunkingStrategy,
        dimensionality: &Dimensionality,
    ) -> ChunkGrid {
        vec![
            chunking_strategy.into(),
            NonZeroU64::new(dimensionality.get_third_term_size()).unwrap(),
        ]
        .into()
    }

    fn fill_value(&self) -> FillValue {
        FillValue::from(0u64)
    }

    fn dimension_names(&self, reference_system: &ReferenceSystem) -> Option<Vec<DimensionName>> {
        match reference_system {
            ReferenceSystem::SPO => Some(vec![
                DimensionName::new("Subjects"),
                DimensionName::new("Objects"),
            ]),
            ReferenceSystem::SOP => Some(vec![
                DimensionName::new("Subjects"),
                DimensionName::new("Predicates"),
            ]),
            ReferenceSystem::PSO => Some(vec![
                DimensionName::new("Predicates"),
                DimensionName::new("Objects"),
            ]),
            ReferenceSystem::POS => Some(vec![
                DimensionName::new("Predicates"),
                DimensionName::new("Subjects"),
            ]),
            ReferenceSystem::OPS => Some(vec![
                DimensionName::new("Objects"),
                DimensionName::new("Subjects"),
            ]),
            ReferenceSystem::OSP => Some(vec![
                DimensionName::new("Objects"),
                DimensionName::new("Predicates"),
            ]),
        }
    }

    fn array_to_bytes_codec(
        &self,
        dimensionality: &Dimensionality,
    ) -> StorageResult<Box<dyn ArrayToBytesCodecTraits>> {
        let mut sharding_codec_builder =
            ShardingCodecBuilder::new(vec![1, dimensionality.get_third_term_size()].try_into()?);
        sharding_codec_builder.bytes_to_bytes_codecs(vec![Box::new(GzipCodec::new(5)?)]);
        Ok(Box::new(sharding_codec_builder.build()))
    }
}

impl LayoutOps<Chunk> for MatrixLayout {
    fn graph_iter(&self, graph: Graph) -> Vec<Chunk> {
        graph
    }

    fn store_chunk_elements(&self, chunk: &[Chunk], columns: usize) -> Vec<u64> {
        // We create a slice that has the size of the chunk filled with 0 values
        // having the size of the shard; that is, number of rows, and a given
        // number of columns. This value is converted into an AtomicU8 for us to
        // be able to share it among threads
        let slice: Vec<AtomicZarrType> = vec![0u64; chunk.len() * columns]
            .iter()
            .map(|&n| AtomicZarrType::new(n))
            .collect();

        for (first_term, triples) in chunk.iter().enumerate() {
            triples.iter().for_each(|&(second_term, third_term)| {
                let third_term_idx = third_term as usize + first_term * columns;
                slice[third_term_idx].store(second_term as u64, Ordering::Relaxed);
            });
        }

        slice
            .iter()
            .map(|elem| elem.load(Ordering::Relaxed))
            .collect::<Vec<u64>>()
    }

    fn retrieve_chunk_elements(
        &mut self,
        matrix: &Mutex<TriMat<usize>>,
        first_term_index: usize,
        chunk: &[usize],
    ) {
        chunk
            .iter()
            .enumerate()
            .for_each(|(third_term_idx, &second_term_idx)| {
                if second_term_idx != 0 {
                    matrix
                        .lock()
                        .add_triplet(first_term_index, third_term_idx, second_term_idx);
                }
            })
    }

    fn sharding_factor(&self, dimensionality: &Dimensionality) -> usize {
        dimensionality.first_term_size
    }
}
