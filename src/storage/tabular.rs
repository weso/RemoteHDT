use parking_lot::Mutex;
use sprs::TriMat;
use zarrs::array::codec::array_to_bytes::sharding::ShardingCodecBuilder;
use zarrs::array::codec::ArrayToBytesCodecTraits;
use zarrs::array::codec::GzipCodec;
use zarrs::array::Array;
use zarrs::array::ChunkGrid;
use zarrs::array::DataType;
use zarrs::array::DimensionName;
use zarrs::array::FillValue;
use zarrs::storage::ReadableStorageTraits;

use crate::io::Graph;

use super::layout::Layout;
use super::layout::LayoutOps;
use super::params::ChunkingStrategy;
use super::params::Dimensionality;
use super::params::ReferenceSystem;
use super::StorageResult;
use super::ZarrArray;

type ZarrType = u64;
type Chunk = (u32, u32, u32);

pub struct TabularLayout;

impl<R> Layout<R, ZarrType, Chunk> for TabularLayout
where
    R: ReadableStorageTraits + Sized,
{
    fn shape(&self, dimensionality: &Dimensionality) -> Vec<u64> {
        vec![dimensionality.get_graph_size(), 3]
    }

    fn data_type(&self) -> DataType {
        DataType::UInt64
    }

    fn chunk_shape(&self, chunking_strategy: ChunkingStrategy, _: &Dimensionality) -> ChunkGrid {
        vec![chunking_strategy.into(), 3].into() // TODO: make this a constant value
    }

    fn fill_value(&self) -> FillValue {
        FillValue::from(0u64)
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
        let mut sharding_codec_builder = ShardingCodecBuilder::new(vec![1, 3]);
        sharding_codec_builder.bytes_to_bytes_codecs(vec![Box::new(GzipCodec::new(5)?)]);
        Ok(Box::new(sharding_codec_builder.build()))
    }
}

impl<R> LayoutOps<R, ZarrType, (u32, u32, u32)> for TabularLayout
where
    R: ReadableStorageTraits + Sized,
{
    fn graph_iter(&self, graph: Graph) -> Vec<Chunk> {
        graph
            .iter()
            .enumerate()
            .flat_map(|(first_term, triples)| {
                triples
                    .iter()
                    .map(|&(second_term, third_term)| (first_term as u32, second_term, third_term))
                    .collect::<Vec<Chunk>>()
            })
            .collect::<Vec<Chunk>>()
    }

    fn chunk_elements(&self, chunk: &[Chunk], _: usize) -> Vec<ZarrType> {
        let mut ans = Vec::new();
        for &(first_term, second_term, third_term) in chunk {
            ans.push(first_term as ZarrType);
            ans.push(second_term as ZarrType);
            ans.push(third_term as ZarrType);
        }
        ans
    }

    fn parse(
        &mut self,
        arr: &Array<R>,
        dimensionality: &Dimensionality,
    ) -> StorageResult<ZarrArray> {
        let matrix = Mutex::new(TriMat::new((
            dimensionality.first_term_size,
            dimensionality.third_term_size,
        )));
        let number_of_chunks = match arr.chunk_grid_shape() {
            Some(chunk_grid) => chunk_grid[0] as usize,
            None => 0,
        };
        (0..number_of_chunks).for_each(|i| {
            // Using this chunking strategy allows us to keep RAM usage low,
            // as we load elements by row
            if let Ok(chunk_elements) = arr.retrieve_chunk_elements::<usize>(&[i as ZarrType, 0]) {
                chunk_elements.chunks(3).for_each(|triple| {
                    matrix
                        .lock()
                        .add_triplet(triple[0], triple[2], triple[1] as u8);
                })
            }
        });

        // We use a CSC Matrix because typically, RDF knowledge graphs tend to
        // have more rows than columns
        let x = matrix.lock();
        Ok(x.to_csc())
    }

    fn sharding_factor(&self, dimensionality: &Dimensionality) -> usize {
        dimensionality.first_term_size * dimensionality.third_term_size
    }
}
