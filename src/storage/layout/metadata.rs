use std::num::NonZeroU64;

use parking_lot::Mutex;
use sprs::TriMat;
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
use crate::storage::Layout;

type Chunk = (u32, u32, u32);

pub struct MetadataLayout;

impl Layout<Chunk> for MetadataLayout {
    fn shape(&self, dimensionality: &Dimensionality) -> Vec<u64> {
        vec![dimensionality.get_graph_size(), 3]
    }

    fn data_type(&self) -> DataType {
        DataType::UInt32
    }

    fn chunk_shape(&self, chunking_strategy: ChunkingStrategy, _: &Dimensionality) -> ChunkGrid {
        vec![chunking_strategy.into(), NonZeroU64::new(3).unwrap()].into() // TODO: make this a constant value
    }

    fn fill_value(&self) -> FillValue {
        FillValue::from(0u32)
    }

    fn dimension_names(&self, _: &ReferenceSystem) -> Option<Vec<DimensionName>> {
        Some(vec![
            DimensionName::new("Triples"),
            DimensionName::new("Fields"),
        ])
    }

    fn array_to_bytes_codec(
        &self,
        _: &Dimensionality,
    ) -> StorageResult<Box<dyn ArrayToBytesCodecTraits>> {
        let mut sharding_codec_builder = ShardingCodecBuilder::new(vec![1, 3].try_into()?);
        sharding_codec_builder.bytes_to_bytes_codecs(vec![Box::new(GzipCodec::new(5)?)]);
        Ok(Box::new(sharding_codec_builder.build()))
    }
}

impl LayoutOps<Chunk> for MetadataLayout {
    fn graph_iter(&self, graph: Graph) -> Vec<Chunk> {
        graph
            .iter()
            .enumerate()
            .flat_map(|(first_term, triples)| {

                let count = 0;
                triples
                    .iter()
                    .map(|&(second_term, third_term)| {
                        (count as u32, 1, 1)
                    })
                    .collect::<Vec<Chunk>>()
            })
            .collect::<Vec<Chunk>>()
    }

    fn store_chunk_elements(&self, chunk: &[Chunk], _: usize) -> Vec<u32> {
        let mut ans = Vec::new();
        for &(first_term, second_term, third_term) in chunk {
            ans.push(first_term);
            ans.push(second_term);
            ans.push(third_term);
        }
        ans
    }

    fn retrieve_chunk_elements(
        &mut self,
        matrix: &Mutex<TriMat<usize>>,
        first_term_index: usize, // TODO: will first_term_index instead of chunk[0] do the trick?
        chunk: &[usize],
    ) {
        matrix
            .lock()
            .add_triplet(chunk[0], chunk[2], chunk[1] as usize);
    }

    fn sharding_factor(&self, dimensionality: &Dimensionality) -> usize {
        dimensionality.first_term_size * dimensionality.third_term_size
    }
}
