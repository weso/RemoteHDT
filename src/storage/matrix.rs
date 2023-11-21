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
use zarrs::storage::store::FilesystemStore;
use zarrs::storage::ReadableStorageTraits;

use super::layout::ArcVec;
use super::layout::AtomicCounter;
use super::layout::Layout;
use super::layout::LayoutOps;
use super::ChunkingStrategy;
use super::StorageResult;
use super::ZarrArray;

use crate::dictionary::Dictionary;
use crate::io::Graph;
use crate::utils::subjects_per_chunk;

pub struct MatrixLayout;

impl MatrixLayout {
    fn append_slice(ans: ArcVec<u8>, triples: &[(String, String)], dictionary: &Dictionary) {
        let slice: Vec<AtomicU8> = vec![0u8; dictionary.objects_size()]
            .iter()
            .map(|&n| AtomicU8::new(n))
            .collect();

        triples.iter().for_each(|(predicate, object)| {
            let pidx = dictionary.get_predicate_idx_unchecked(&predicate.to_string());
            let oidx = dictionary.get_object_idx_unchecked(&object.to_string());

            slice[oidx].store(pidx as u8, Ordering::Relaxed);
        });

        ans.lock().unwrap().append(
            &mut slice
                .iter()
                .map(|elem| elem.load(Ordering::Relaxed))
                .collect::<Vec<u8>>(),
        )
    }
}

impl<R> Layout<R, u8> for MatrixLayout
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

impl<R> LayoutOps<R, u8> for MatrixLayout
where
    R: ReadableStorageTraits + Sized,
{
    fn serialize_graph(
        &mut self,
        arr: &Array<FilesystemStore>,
        dictionary: &Dictionary,
        graph: Graph,
        ans: ArcVec<u8>,
        count: AtomicCounter,
        chunk_x: u64,
        _chunk_y: u64,
    ) -> StorageResult<u64> {
        dictionary.subjects().iter().for_each(|(_, subject)| {
            let subject = &std::str::from_utf8(&subject).unwrap().to_string();
            let triples = graph.get(subject).unwrap();

            MatrixLayout::append_slice(ans.to_owned(), triples, dictionary);

            if (count.load(Ordering::Relaxed) + 1) % chunk_x == 0 {
                arr.store_chunk_elements(
                    &[count.load(Ordering::Relaxed), 0],
                    ans.lock().unwrap().as_slice(),
                )
                .unwrap();
                ans.lock().unwrap().clear();
                count.fetch_add(1, Ordering::Relaxed);
            }
        });

        Ok(count.load(Ordering::Relaxed))
    }

    fn parse(&mut self, arr: Array<R>, dictionary: &Dictionary) -> StorageResult<ZarrArray> {
        let matrix = Mutex::new(TriMat::new((
            dictionary.subjects_size(),
            dictionary.objects_size(),
        )));
        (0..arr.chunk_grid_shape().unwrap()[0]).for_each(|i| {
            // Using this chunking strategy allows us to keep RAM usage low,
            // as we load elements by row
            arr.retrieve_chunk_elements::<u8>(&[i, 0])
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
}
