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
use super::MetadataStructure;
use super::ReferenceSystem;
use super::MetadataResult;
use super::Structure;
use super::StructureOps;

use crate::io::Graph;
use crate::metadata::params::MetadataDimensionality;



type Chunk = Vec<u32>;

pub struct CoordinatesStructure;

impl Structure<Chunk> for CoordinatesStructure {
    fn shape(&self, dimensionality: &MetadataDimensionality) -> Vec<u64> {
        vec![dimensionality.get_metadata_size(), dimensionality.get_fields_size()]
    }

    fn data_type(&self) -> DataType {
        DataType::UInt32
    }

    fn chunk_shape(&self, chunking_strategy: ChunkingStrategy, dimensionality: &MetadataDimensionality) -> ChunkGrid {
        vec![chunking_strategy.into(), NonZeroU64::new(dimensionality.get_fields_size()).unwrap()].into() // TODO: make this a constant value
    }

    fn fill_value(&self) -> FillValue {
        FillValue::from(0u32)
    }

    fn dimension_names(&self) -> Option<Vec<DimensionName>> {
        Some(vec![
            DimensionName::new("Triples_metadata"),
            DimensionName::new("Fields"),
        ])
    }

    fn array_to_bytes_codec(
        &self,
        dimensionality: &MetadataDimensionality,
    ) -> MetadataResult<Box<dyn ArrayToBytesCodecTraits>> {
        let mut sharding_codec_builder = ShardingCodecBuilder::new(vec![1, dimensionality.get_fields_size()].try_into()?);
        sharding_codec_builder.bytes_to_bytes_codecs(vec![Box::new(GzipCodec::new(5)?)]);
        Ok(Box::new(sharding_codec_builder.build()))
    }
}

impl StructureOps<Chunk> for CoordinatesStructure {


    fn store_chunk_elements(&self, chunk: &[Chunk], _: usize) -> Vec<u32> {
        let mut ans = Vec::new();
        for i in chunk {
            for &j in i {
                ans.push(j);
            }
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

    fn sharding_factor(&self, dimensionality: &MetadataDimensionality) -> usize {
        dimensionality.metadata_size * dimensionality.fields_size
    }

    fn metadata_iter(&self, metadata_structure: MetadataStructure) -> Vec<Chunk> {
       metadata_structure
    }
}
