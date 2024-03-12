use parking_lot::Mutex;
use sprs::CsMat;
use sprs::TriMat;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use zarrs::array::codec::ArrayToBytesCodecTraits;
use zarrs::array::Array;
use zarrs::array::ChunkGrid;
use zarrs::array::DataType;
use zarrs::array::DimensionName;
use zarrs::array::FillValue;
use zarrs::array_subset::ArraySubset;
use zarrs::storage::store::OpendalStore;

use crate::dictionary::Dictionary;
use crate::error::RemoteHDTError;
use crate::io::Graph;
use crate::utils::columns_per_shard;
use crate::utils::rows_per_shard;
use crate::utils::value_to_term;

use super::params::MetadataDimensionality;
use super::ChunkingStrategy;
use super::Dimensionality;
use super::ReferenceSystem;

use super::MetadataResult;

type ArrayToBytesCodec = Box<dyn ArrayToBytesCodecTraits>;

pub mod coordinates;

type MetadataStructure = Vec<Vec<u32>>;

pub trait StructureOps<C> {
    fn retrieve_attributes(&mut self, arr: &Array<OpendalStore>){
       
    }

    fn serialize(&mut self, arr: Array<OpendalStore>, metadata: MetadataStructure) -> MetadataResult<()> {
        let columns = arr.shape()[1] as usize;
        let count = AtomicU64::new(0);
        let binding = self.metadata_iter(metadata.to_owned());
        let iter = binding.chunks_exact(rows_per_shard(&arr) as usize);
        let remainder = iter.remainder();

        for chunk in iter {
            let slice = self.store_chunk_elements(chunk, columns);
            arr.store_chunk_elements(&[count.load(Ordering::Relaxed), 0], slice)?;
            count.fetch_add(1, Ordering::Relaxed);
        }

        if !remainder.is_empty() {
            arr.store_array_subset_elements(
                &ArraySubset::new_with_start_shape(
                    vec![count.load(Ordering::Relaxed) * rows_per_shard(&arr), 0],
                    vec![remainder.len() as u64, columns_per_shard(&arr)],
                )?,
                self.store_chunk_elements(remainder, columns),
            )?;
        }

        Ok(())
    }

    fn parse(
        &mut self,
        arr: &Array<OpendalStore>,
        dimensionality: &MetadataDimensionality,
    ) {
    }
    
    fn metadata_iter(&self, graph: MetadataStructure) -> Vec<C>;
    fn store_chunk_elements(&self, chunk: &[C], columns: usize) -> Vec<u32>;
    fn retrieve_chunk_elements(
        &mut self,
        matrix: &Mutex<TriMat<usize>>,
        first_term_idx: usize,
        chunk: &[usize],
    );
    fn sharding_factor(&self, dimensionality: &MetadataDimensionality) -> usize;
    
}

pub trait Structure<C>: StructureOps<C> {

    fn shape(&self, dimensionality: &MetadataDimensionality) -> Vec<u64>;
    fn data_type(&self) -> DataType;
    fn chunk_shape(
        &self,
        chunking_strategy: ChunkingStrategy,
        dimensionality: &MetadataDimensionality,
    ) -> ChunkGrid;
    fn fill_value(&self) -> FillValue;
    fn dimension_names(&self) -> Option<Vec<DimensionName>>;
    fn array_to_bytes_codec(
        &self,
        dimensionality: &MetadataDimensionality,
    ) -> MetadataResult<ArrayToBytesCodec>;
}
