use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::sync::Mutex;

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
pub(crate) type AtomicCounter = Arc<AtomicU64>;
pub(crate) type ArcVec<T> = Arc<Mutex<Vec<T>>>;

pub trait LayoutOps<R, T: TriviallyTransmutable> {
    fn retrieve_attributes(&mut self, arr: &Array<R>) -> Dictionary {
        // 4. We get the attributes so we can obtain some values that we will need
        let attributes = arr.attributes();

        let subjects = &value_to_term(attributes.get("subjects").unwrap());
        let predicates = &value_to_term(attributes.get("predicates").unwrap());
        let objects = &value_to_term(attributes.get("objects").unwrap());

        Dictionary::from_vec_str(subjects, predicates, objects)
    }

    fn serialize(
        &mut self,
        arr: Array<FilesystemStore>,
        dictionary: &Dictionary,
        graph: Graph,
    ) -> StorageResult<()> {
        let ans = Arc::new(Mutex::new(Vec::<T>::new()));

        let count = self.serialize_graph(
            &arr,
            dictionary,
            graph,
            ans.to_owned(),
            Arc::new(AtomicU64::new(0)),
            subjects_per_chunk(&arr),
            objects_per_chunk(&arr),
        )?;

        let ans = ans.lock().unwrap();
        if !ans.is_empty() {
            arr.store_array_subset_elements(
                &ArraySubset::new_with_start_shape(
                    vec![count * subjects_per_chunk(&arr), 0],
                    vec![
                        ans.len() as u64 / objects_per_chunk(&arr),
                        objects_per_chunk(&arr),
                    ],
                )
                .unwrap(), // TODO: remove unwrap
                ans.as_slice(),
            )
            .unwrap();
        }

        Ok(())
    }

    fn serialize_graph(
        &mut self,
        arr: &Array<FilesystemStore>,
        dictionary: &Dictionary,
        graph: Graph,
        ans: ArcVec<T>,
        count: AtomicCounter,
        chunk_x: u64,
        chunk_y: u64,
    ) -> StorageResult<u64>;

    fn parse(&mut self, arr: Array<R>, dictionary: &Dictionary) -> StorageResult<ZarrArray>;
}

pub trait Layout<R, T: TriviallyTransmutable>: LayoutOps<R, T> {
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
