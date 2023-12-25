use parking_lot::Mutex;
use sprs::TriMat;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use zarrs::array::codec::array_to_bytes::sharding::ShardingCodecBuilder;
use zarrs::array::codec::ArrayToBytesCodecTraits;
use zarrs::array::codec::GzipCodec;
use zarrs::array::Array;
use zarrs::array::ChunkGrid;
use zarrs::array::DataType;
use zarrs::array::DimensionName;
use zarrs::array::FillValue;
use zarrs::storage::ReadableStorageTraits;

use super::layout::Layout;
use super::layout::LayoutOps;
use super::ChunkingStrategy;
use super::Dimensionality;
use super::ReferenceSystem;
use super::StorageResult;
use super::ZarrArray;

use crate::io::Graph;
use crate::utils::rows_per_shard;

type ZarrType = u8;
type Chunk = Vec<(u32, u32)>;

pub struct MatrixLayout;

impl<R> Layout<R, ZarrType, Chunk> for MatrixLayout
where
    R: ReadableStorageTraits + Sized,
{
    fn shape(&self, dimensionality: &Dimensionality) -> Vec<u64> {
        vec![
            dimensionality.get_first_term_size(),
            dimensionality.get_third_term_size(),
        ]
    }

    fn data_type(&self) -> DataType {
        DataType::UInt8
    }

    fn chunk_shape(
        &self,
        chunking_strategy: ChunkingStrategy,
        dimensionality: &Dimensionality,
    ) -> ChunkGrid {
        vec![
            chunking_strategy.into(),
            dimensionality.get_third_term_size(),
        ]
        .into()
    }

    fn fill_value(&self) -> FillValue {
        FillValue::from(0u8)
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
            ShardingCodecBuilder::new(vec![1, dimensionality.get_third_term_size()]);
        sharding_codec_builder.bytes_to_bytes_codecs(vec![Box::new(GzipCodec::new(5)?)]);
        Ok(Box::new(sharding_codec_builder.build()))
    }
}

impl<R> LayoutOps<R, ZarrType, Chunk> for MatrixLayout
where
    R: ReadableStorageTraits + Sized,
{
    fn graph_iter(&self, graph: Graph) -> Vec<Chunk> {
        graph
    }

    fn chunk_elements(&self, chunk: &[Chunk], columns: usize) -> Vec<ZarrType> {
        // We create a slice that has the size of the chunk filled with 0 values
        // having the size of the shard; that is, number of rows, and a given
        // number of columns. This value is converted into an AtomicU8 for us to
        // be able to share it among threads
        let slice: Vec<AtomicU8> = vec![0u8; chunk.len() * columns]
            .iter()
            .map(|&n| AtomicU8::new(n))
            .collect();

        for (first_term, triples) in chunk.iter().enumerate() {
            triples.iter().for_each(|&(second_term, third_term)| {
                let third_term_idx = third_term as usize + first_term * columns;
                slice[third_term_idx].store(second_term as ZarrType, Ordering::Relaxed);
            });
        }

        slice
            .iter()
            .map(|elem| elem.load(Ordering::Relaxed))
            .collect::<Vec<ZarrType>>()
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
            Some(chunk_grid) => chunk_grid[0],
            None => 0,
        };
        (0..number_of_chunks).for_each(|i| {
            // Using this chunking strategy allows us to keep RAM usage low,
            // as we load elements by row
            arr.retrieve_chunk_elements::<ZarrType>(&[i, 0])
                .unwrap()
                .chunks(dimensionality.third_term_size)
                .enumerate()
                .for_each(|(first_term_idx, chunk)| {
                    chunk
                        .iter()
                        .enumerate()
                        .for_each(|(third_term_idx, &second_term_idx)| {
                            if second_term_idx != 0 {
                                matrix.lock().add_triplet(
                                    first_term_idx + (i * rows_per_shard(arr)) as usize,
                                    third_term_idx,
                                    second_term_idx,
                                );
                            }
                        })
                })
        });

        // We use a CSC Matrix because typically, RDF knowledge graphs tend to
        // have more rows than columns
        let x = matrix.lock();
        Ok(x.to_csc())
    }

    fn sharding_factor(&self, dimensionality: &Dimensionality) -> usize {
        dimensionality.first_term_size
    }
}
