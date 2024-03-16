use parking_lot::Mutex;
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
use zarrs::storage::store::FilesystemStore;
use zarrs::storage::ReadableStorageTraits;

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

pub mod matrix;
pub mod tabular;

pub trait LayoutOps<C> {
    fn retrieve_attributes(
        &mut self,
        arr: &Array<dyn ReadableStorageTraits>,
    ) -> StorageResult<Dictionary> {
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

    fn serialize(&mut self, arr: &Array<FilesystemStore>, graph: Graph) -> StorageResult<()> {
        let columns = arr.shape()[1] as usize;
        let count = AtomicU64::new(0);
        let binding = self.graph_iter(graph.to_owned());
        let iter = binding.chunks_exact(rows_per_shard(arr) as usize);
        let remainder = iter.remainder();

        for chunk in iter {
            let slice = self.store_chunk_elements(chunk, columns);
            arr.store_chunk_elements::<u32>(&[count.load(Ordering::Relaxed), 0], slice)?;
            count.fetch_add(1, Ordering::Relaxed);
        }

        if !remainder.is_empty() {
            // first we count the number of shards that have been processed, and
            // multiply it by the number of chunks in every shard. Hence, we will
            // obtain the number of rows that have been processed
            let rows_processed = count.load(Ordering::Relaxed) * rows_per_shard(arr);
            // then we obtain the size of the last shard that is going to be
            // processed; it is equals to the size of the remainder
            let last_shard_size = remainder.len() as u64;
            // lastly, we store the elements in the provided subset
            arr.store_array_subset_elements::<u32>(
                &ArraySubset::new_with_start_shape(
                    vec![rows_processed, 0],
                    vec![last_shard_size, columns_per_shard(arr)],
                )?,
                self.store_chunk_elements(remainder, columns),
            )?;
        }

        Ok(())
    }

    fn parse(
        &mut self,
        arr: &Array<dyn ReadableStorageTraits>,
        dimensionality: &Dimensionality,
    ) -> StorageResult<ZarrArray> {
        // First, we create the 2D matrix in such a manner that the number of
        // rows is the same as the size of the first terms; i.e, in the SPO
        // orientation, that will be equals to the number of subjects, while
        // the number of columns is equals to the size of the third terms; i.e,
        // following the same example as before, it will be equals to the number
        // of objects. In our case the dimensionality abstracts the process
        // of getting the size of the concrete dimension
        let matrix = Mutex::new(TriMat::new((
            dimensionality.first_term_size, // we obtain the size of the first terms
            dimensionality.third_term_size, // we obtain the size of the third terms
        )));

        // We compute the number of shards; for us to achieve so, we have to obtain
        // first dimension of the chunk grid
        let number_of_shards = match arr.chunk_grid_shape() {
            Some(chunk_grid) => chunk_grid[0],

            None => 0,
        };

        let number_of_columns = arr.shape()[1] as usize;

        // For each chunk in the Zarr array we retrieve it and parse it into a
        // matrix, inserting the triplet in its corresponding position. The idea
        // of parsing the array chunk-by-chunk allows us to keep the RAM usage
        // low, as instead of parsing the whole array, we process smaller pieces
        // of it. Once we have all the pieces processed, we will have parsed the
        // whole array
        for shard in 0..number_of_shards {
            arr.retrieve_chunk_elements::<u32>(&[shard, 0])?
                // We divide each shard by the number of columns, as a shard is
                // composed of chunks having the size of [1, number of cols]
                .chunks(number_of_columns)
                .enumerate()
                .for_each(|(first_term_idx, chunk)| {
                    self.retrieve_chunk_elements(
                        &matrix,
                        first_term_idx + (shard * rows_per_shard(arr)) as usize,
                        chunk,
                    );
                })
        }

        // We use a CSC Matrix because typically, RDF knowledge graphs tend to
        // have more rows than columns; as such, CSC matrices are optimized
        // for that precise scenario
        let x = matrix.lock();
        Ok(x.to_csc())
    }

    fn graph_iter(&self, graph: Graph) -> Vec<C>;
    fn store_chunk_elements(&self, chunk: &[C], columns: usize) -> Vec<u32>;
    fn retrieve_chunk_elements(
        &mut self,
        matrix: &Mutex<TriMat<usize>>,
        first_term_idx: usize,
        chunk: &[u32],
    );
    fn sharding_factor(&self, dimensionality: &Dimensionality) -> usize;
}

pub trait Layout<C>: LayoutOps<C> {
    fn name(&self) -> String;
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
