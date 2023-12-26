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
use crate::error::RemoteHDTError;
use crate::io::Graph;
use crate::utils::columns_per_shard;
use crate::utils::rows_per_shard;
use crate::utils::value_to_term;

use super::ChunkingStrategy;
use super::Dimensionality;
use super::ReferenceSystem;
use super::StorageResult;
use super::ZarrArray;

type ArrayToBytesCodec = Box<dyn ArrayToBytesCodecTraits>;

pub trait LayoutOps<R, T: TriviallyTransmutable, C> {
    fn retrieve_attributes(&mut self, arr: &Array<R>) -> StorageResult<Dictionary> {
        // 4. We get the attributes so we can obtain some values that we will need
        let attributes = arr.attributes();

        let subjects = &value_to_term(match attributes.get("subjects") {
            Some(subjects) => subjects,
            None => return Err(RemoteHDTError::SubjectsNotInJSON),
        });
        let predicates = &value_to_term(match attributes.get("predicates") {
            Some(predicates) => predicates,
            None => return Err(RemoteHDTError::PredicatesNotInJSON),
        });
        let objects = &value_to_term(match attributes.get("objects") {
            Some(objects) => objects,
            None => return Err(RemoteHDTError::ObjectsNotInJSON),
        });

        let reference_system: ReferenceSystem = match attributes.get("reference_system") {
            Some(reference_system) => reference_system,
            None => return Err(RemoteHDTError::ReferenceSystemNotInJSON),
        }
        .as_str()
        .unwrap()
        .into();

        Ok(Dictionary::from_vec_str(
            reference_system,
            subjects,
            predicates,
            objects,
        ))
    }

    fn serialize(&mut self, arr: Array<FilesystemStore>, graph: Graph) -> StorageResult<()> {
        let columns = arr.shape()[1] as usize;
        let count = AtomicU64::new(0);
        let binding = self.graph_iter(graph.to_owned());
        let iter = binding.chunks_exact(rows_per_shard(&arr) as usize);
        let remainder = iter.remainder();

        for chunk in iter {
            if arr
                .store_chunk_elements(
                    &[count.load(Ordering::Relaxed), 0],
                    self.chunk_elements(chunk, columns),
                )
                .is_err()
            {
                return Err(RemoteHDTError::TripleSerialization);
            }

            count.fetch_add(1, Ordering::Relaxed);
        }

        if !remainder.is_empty() {
            arr.store_array_subset_elements(
                &ArraySubset::new_with_start_shape(
                    vec![count.load(Ordering::Relaxed) * rows_per_shard(&arr), 0],
                    vec![remainder.len() as u64, columns_per_shard(&arr)],
                )?,
                self.chunk_elements(remainder, columns),
            )?;
        }

        Ok(())
    }

    fn graph_iter(&self, graph: Graph) -> Vec<C>;
    fn chunk_elements(&self, chunk: &[C], columns: usize) -> Vec<T>;
    fn parse(
        &mut self,
        arr: &Array<R>,
        dimensionality: &Dimensionality,
    ) -> StorageResult<ZarrArray>;
    fn sharding_factor(&self, dimensionality: &Dimensionality) -> usize;
}

pub trait Layout<R, T: TriviallyTransmutable, C>: LayoutOps<R, T, C> {
    fn shape(&self, dimensionality: &Dimensionality) -> Vec<u64>;
    fn data_type(&self) -> DataType;
    fn chunk_shape(
        &self,
        chunking_strategy: ChunkingStrategy,
        dimensionality: &Dimensionality,
    ) -> ChunkGrid;
    fn fill_value(&self) -> FillValue;
    fn dimension_names(&self, reference_system: &ReferenceSystem) -> Option<Vec<DimensionName>>;
    fn array_to_bytes_codec(
        &self,
        dimensionality: &Dimensionality,
    ) -> StorageResult<ArrayToBytesCodec>;
}
