use sprs::TriMat;
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
use zarrs::storage::store::FilesystemStore;
use zarrs::storage::ReadableStorageTraits;

use crate::dictionary::Dictionary;
use crate::io::Graph;

use super::layout::ArcVec;
use super::layout::AtomicCounter;
use super::layout::Layout;
use super::layout::LayoutOps;
use super::ChunkingStrategy;
use super::StorageResult;
use super::ZarrArray;

pub struct TabularLayout;

impl TabularLayout {
    fn insert_triple(
        ans: ArcVec<u64>,
        subject: &str,
        predicate: String,
        object: String,
        dictionary: &Dictionary,
    ) {
        ans.lock()
            .unwrap()
            .push(dictionary.get_subject_idx_unchecked(subject) as u64);
        ans.lock()
            .unwrap()
            .push(dictionary.get_predicate_idx_unchecked(&predicate) as u64);
        ans.lock()
            .unwrap()
            .push(dictionary.get_object_idx_unchecked(&object) as u64);
    }
}

impl<R> Layout<R, u64> for TabularLayout
where
    R: ReadableStorageTraits + Sized,
{
    fn shape(&self, _dictionary: &Dictionary, graph: &Graph) -> Vec<u64> {
        vec![
            graph
                .iter()
                .map(|(_, triples)| triples.len() as u64)
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

impl<R> LayoutOps<R, u64> for TabularLayout
where
    R: ReadableStorageTraits + Sized,
{
    fn serialize_graph(
        &mut self,
        arr: &Array<FilesystemStore>,
        dictionary: &Dictionary,
        graph: Graph,
        ans: ArcVec<u64>,
        count: AtomicCounter,
        chunk_x: u64,
        chunk_y: u64,
    ) -> StorageResult<u64> {
        graph.iter().for_each(|(subject, triples)| {
            for (predicate, object) in triples {
                TabularLayout::insert_triple(
                    ans.to_owned(),
                    subject,
                    predicate.to_owned(),
                    object.to_owned(),
                    dictionary,
                );

                if ans.lock().unwrap().len() == (chunk_x * chunk_y) as usize {
                    arr.store_chunk_elements(
                        &[count.load(Ordering::Relaxed), 0],
                        ans.lock().unwrap().as_slice(),
                    )
                    .unwrap(); // TODO: remove unwrap
                    ans.lock().unwrap().clear();
                    count.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        Ok(count.load(Ordering::Relaxed))
    }

    fn parse(&mut self, arr: Array<R>, dictionary: &Dictionary) -> StorageResult<ZarrArray> {
        let matrix = Mutex::new(TriMat::new((
            dictionary.subjects_size(),
            dictionary.objects_size(),
        )));
        (0..arr.chunk_grid_shape().unwrap()[0] as usize).for_each(|i| {
            // Using this chunking strategy allows us to keep RAM usage low,
            // as we load elements by row
            arr.retrieve_chunk_elements::<usize>(&[i as u64, 0])
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
}
