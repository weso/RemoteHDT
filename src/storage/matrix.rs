use sprs::TriMat;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
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

use super::layout::Layout;
use super::layout::LayoutOps;
use super::ChunkingStrategy;
use super::StorageResult;
use super::ZarrArray;

use crate::dictionary::Dictionary;
use crate::io::Graph;
use crate::utils::subjects_per_chunk;

type ZarrType = u8;
type Chunk = Vec<(u32, u32)>;

pub struct MatrixLayout;

impl<R> Layout<R, ZarrType, Chunk> for MatrixLayout
where
    R: ReadableStorageTraits + Sized,
{
    fn shape(&self, dictionary: &Dictionary, _graph: &Graph) -> Vec<u64> {
        vec![
            dictionary.subjects_size() as u64,
            dictionary.objects_size() as u64,
        ]
    }

    fn data_type(&self) -> DataType {
        DataType::UInt8
    }

    fn chunk_shape(
        &self,
        chunking_strategy: ChunkingStrategy,
        dictionary: &Dictionary,
    ) -> ChunkGrid {
        vec![chunking_strategy.into(), dictionary.objects_size() as u64].into()
    }

    fn fill_value(&self) -> FillValue {
        FillValue::from(0u8)
    }

    fn dimension_names(&self) -> Option<Vec<DimensionName>> {
        Some(vec![
            DimensionName::new("Subjects"),
            DimensionName::new("Objects"),
        ])
    }

    fn array_to_bytes_codec(
        &self,
        dictionary: &Dictionary,
    ) -> StorageResult<Box<dyn ArrayToBytesCodecTraits>> {
        let mut sharding_codec_builder =
            ShardingCodecBuilder::new(vec![1, dictionary.objects_size() as u64]);
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

    fn chunk_elements(&self, chunk: &[Chunk], objects: usize) -> Vec<ZarrType> {
        let slice: Vec<AtomicU8> = vec![0u8; chunk.len() * objects]
            .iter()
            .map(|&n| AtomicU8::new(n))
            .collect();

        for (i, triples) in chunk.iter().enumerate() {
            triples.iter().for_each(|&(predicate, object)| {
                let object_idx = object as usize + i * objects;
                slice[object_idx].store(predicate as ZarrType, Ordering::Relaxed);
            });
        }

        slice
            .iter()
            .map(|elem| elem.load(Ordering::Relaxed))
            .collect::<Vec<ZarrType>>()
    }

    fn parse(&mut self, arr: Array<R>, dictionary: &Dictionary) -> StorageResult<ZarrArray> {
        let matrix = Mutex::new(TriMat::new((
            dictionary.subjects_size(),
            dictionary.objects_size(),
        )));
        (0..arr.chunk_grid_shape().unwrap()[0]).for_each(|i| {
            // Using this chunking strategy allows us to keep RAM usage low,
            // as we load elements by row
            arr.retrieve_chunk_elements::<ZarrType>(&[i, 0])
                .unwrap()
                .chunks(dictionary.objects_size())
                .enumerate()
                .for_each(|(subject_idx, chunk)| {
                    chunk
                        .iter()
                        .enumerate()
                        .for_each(|(object_idx, &predicate_idx)| {
                            if predicate_idx != 0 {
                                matrix.lock().unwrap().add_triplet(
                                    subject_idx + (i * subjects_per_chunk(&arr)) as usize,
                                    object_idx,
                                    predicate_idx,
                                );
                            }
                        })
                })
        });

        // We use a CSC Matrix because typically, RDF knowledge graphs tend to
        // have more rows than columns
        let x = matrix.lock().unwrap();
        Ok(x.to_csc())
    }

    fn sharding_factor(&self, subjects: usize, _: usize) -> usize {
        subjects
    }
}
