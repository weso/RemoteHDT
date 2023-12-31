use sprs::TriMat;
use std::sync::Mutex;
use zarrs::array::codec::array_to_bytes::sharding::ShardingCodecBuilder;
use zarrs::array::codec::ArrayToBytesCodecTraits;
use zarrs::array::codec::GzipCodec;
use zarrs::array::Array;
use zarrs::array::ChunkGrid;
use zarrs::array::DataType;
use zarrs::array::DimensionName;
use zarrs::array::FillValue;
use zarrs::storage::ReadableStorageTraits;

use crate::dictionary::Dictionary;
use crate::io::Graph;

use super::layout::Layout;
use super::layout::LayoutOps;
use super::ChunkingStrategy;
use super::StorageResult;
use super::ZarrArray;

type ZarrType = u64;
type Chunk = (u32, u32, u32);

pub struct TabularLayout;

impl<R> Layout<R, ZarrType, Chunk> for TabularLayout
where
    R: ReadableStorageTraits + Sized,
{
    fn shape(&self, _dictionary: &Dictionary, graph: &Graph) -> Vec<u64> {
        vec![
            graph
                .iter()
                .map(|triples| triples.len() as u64)
                .reduce(|acc, a| acc + a)
                .unwrap(),
            3,
        ]
    }

    fn data_type(&self) -> DataType {
        DataType::UInt64
    }

    fn chunk_shape(
        &self,
        chunking_strategy: ChunkingStrategy,
        _dictionary: &Dictionary,
    ) -> ChunkGrid {
        vec![chunking_strategy.into(), 3].into() // TODO: make this a constant value
    }

    fn fill_value(&self) -> FillValue {
        FillValue::from(0u64)
    }

    fn dimension_names(&self) -> Option<Vec<DimensionName>> {
        Some(vec![
            DimensionName::new("Triples"),
            DimensionName::new("Fields"),
        ])
    }

    fn array_to_bytes_codec(
        &self,
        _dictionary: &Dictionary,
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
            .flat_map(|(subject, triples)| {
                triples
                    .iter()
                    .map(|&(predicate, object)| (subject as u32, predicate, object))
                    .collect::<Vec<Chunk>>()
            })
            .collect::<Vec<Chunk>>()
    }

    fn chunk_elements(&self, chunk: &[Chunk], _: usize) -> Vec<ZarrType> {
        let mut ans = Vec::new();
        for &(subject, predicate, object) in chunk {
            ans.push(subject as ZarrType);
            ans.push(predicate as ZarrType);
            ans.push(object as ZarrType);
        }
        ans
    }

    fn parse(&mut self, arr: Array<R>, dictionary: &Dictionary) -> StorageResult<ZarrArray> {
        let matrix = Mutex::new(TriMat::new((
            dictionary.subjects_size(),
            dictionary.objects_size(),
        )));
        (0..arr.chunk_grid_shape().unwrap()[0] as usize).for_each(|i| {
            // Using this chunking strategy allows us to keep RAM usage low,
            // as we load elements by row
            arr.retrieve_chunk_elements::<usize>(&[i as ZarrType, 0])
                .unwrap()
                .chunks(3)
                .for_each(|triple| {
                    matrix
                        .lock()
                        .unwrap()
                        .add_triplet(triple[0], triple[2], triple[1] as u8);
                })
        });

        // We use a CSC Matrix because typically, RDF knowledge graphs tend to
        // have more rows than columns
        let x = matrix.lock().unwrap();
        Ok(x.to_csc())
    }

    fn sharding_factor(&self, subjects: usize, objects: usize) -> usize {
        subjects * objects
    }
}
