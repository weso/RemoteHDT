use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use safe_transmute::TriviallyTransmutable;
use zarrs::array::codec::ArrayToBytesCodecTraits;
use zarrs::array::Array;
use zarrs::array::ChunkGrid;
use zarrs::array::DataType;
use zarrs::array::DimensionName;
use zarrs::array::FillValue;
use zarrs::array_subset::ArraySubset;
use zarrs::storage::store::FilesystemStore;

use crate::dictionary::Dictionary;
use crate::io::Graph;
use crate::utils::objects_per_chunk;
use crate::utils::subjects_per_chunk;
use crate::utils::value_to_term;

use super::ChunkingStrategy;
use super::StorageResult;
use super::ZarrArray;

type ArrayToBytesCodec = Box<dyn ArrayToBytesCodecTraits>;

pub trait LayoutOps<R, T: TriviallyTransmutable, C> {
    fn retrieve_attributes(&mut self, arr: &Array<R>) -> Dictionary {
        // 4. We get the attributes so we can obtain some values that we will need
        let attributes = arr.attributes();

        let subjects = &value_to_term(attributes.get("subjects").unwrap());
        let predicates = &value_to_term(attributes.get("predicates").unwrap());
        let objects = &value_to_term(attributes.get("objects").unwrap());

        Dictionary::from_vec_str(subjects, predicates, objects)
    }

    fn serialize(&mut self, arr: Array<FilesystemStore>, graph: Graph) -> StorageResult<()> {
        let objects_size = arr.shape()[1] as usize;
        let count = AtomicU64::new(0);
        let binding = self.graph_iter(graph);
        let iter = binding.chunks_exact(subjects_per_chunk(&arr) as usize);
        let remainder = iter.remainder();

        iter.for_each(|chunk| {
            arr.store_chunk_elements(
                &[count.load(Ordering::Relaxed), 0],
                self.chunk_elements(chunk, objects_size),
            )
            .unwrap();
            count.fetch_add(1, Ordering::Relaxed);
        });

        if !remainder.is_empty() {
            arr.store_array_subset_elements(
                &ArraySubset::new_with_start_shape(
                    vec![count.load(Ordering::Relaxed) * subjects_per_chunk(&arr), 0],
                    vec![remainder.len() as u64, objects_per_chunk(&arr)],
                )
                .unwrap(), // TODO: remove unwrap
                self.chunk_elements(remainder, objects_size),
            )
            .unwrap();
        }

        Ok(())
    }

    fn graph_iter(&self, graph: Graph) -> Vec<C>;
    fn chunk_elements(&self, chunk: &[C], objects: usize) -> Vec<T>;
    fn parse(&mut self, arr: Array<R>, dictionary: &Dictionary) -> StorageResult<ZarrArray>;
    fn sharding_factor(&self, subjects: usize, objects: usize) -> usize;
}

pub trait Layout<R, T: TriviallyTransmutable, C>: LayoutOps<R, T, C> {
    fn shape(&self, dictionary: &Dictionary, graph: &Graph) -> Vec<u64>;
    fn data_type(&self) -> DataType;
    fn chunk_shape(
        &self,
        chunking_strategy: ChunkingStrategy,
        dictionary: &Dictionary,
    ) -> ChunkGrid;
    fn fill_value(&self) -> FillValue;
    fn dimension_names(&self) -> Option<Vec<DimensionName>>;
    fn array_to_bytes_codec(&self, dictionary: &Dictionary) -> StorageResult<ArrayToBytesCodec>;
}
