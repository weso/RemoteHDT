use oxigraph::model::NamedNode;
use oxigraph::model::Subject;
use oxigraph::model::Term;
use proc_macros::Layout;
use rayon::iter::ParallelBridge;
use rayon::prelude::ParallelIterator;
use sprs::TriMat;
use std::sync::Mutex;
use zarrs::array::codec::array_to_bytes::sharding::ShardingCodecBuilder;
use zarrs::array::codec::ArrayToBytesCodecTraits;
use zarrs::array::codec::BytesToBytesCodecTraits;
use zarrs::array::codec::GzipCodec;
use zarrs::array::Array;
use zarrs::array::ChunkGrid;
use zarrs::array::DataType;
use zarrs::array::DimensionName;
use zarrs::array::FillValue;
use zarrs::array_subset::ArraySubset;
use zarrs::storage::ReadableStorageTraits;
use zarrs::storage::WritableStorageTraits;

use crate::io::Graph;

use super::dictionary::Dictionary;
use super::private::LayoutConfiguration;
use super::private::LayoutFields;
use super::private::LayoutOps;
use super::StorageResult;
use super::ZarrArray;

#[derive(Default, Layout)]
pub struct TabularLayout {
    dictionary: Dictionary,
    graph: Graph,
    rdf_path: String,
}

impl TabularLayout {
    fn insert_triple(subject: &Subject, predicate: NamedNode, object: Term, ans: &mut Vec<u64>, dictionary: &Dictionary) {
        ans.push(dictionary.get_subject_idx_unchecked(&subject.to_string()) as u64);
        ans.push(dictionary.get_predicate_idx_unchecked(&predicate.to_string()) as u64);
        ans.push(dictionary.get_object_idx_unchecked(&object.to_string()) as u64);
    }
}

impl LayoutConfiguration for TabularLayout {
    fn shape(&self) -> Vec<u64> {
        vec![self.get_dictionary().subjects_size() as u64, 3]
    }

    fn data_type(&self) -> DataType {
        DataType::UInt64
    }

    fn chunk_shape(&self) -> ChunkGrid {
        vec![1024, 3].into() // TODO: make this a constant value
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

    fn array_to_bytes_codec(&self) -> StorageResult<Box<dyn ArrayToBytesCodecTraits>> {
        let mut sharding_codec_builder = ShardingCodecBuilder::new(vec![1, 3]);
        sharding_codec_builder.bytes_to_bytes_codecs(vec![Box::new(GzipCodec::new(5)?)]);
        Ok(Box::new(sharding_codec_builder.build()))
    }

    fn bytes_to_bytes_codec(&self) -> Vec<Box<dyn BytesToBytesCodecTraits>> {
        Default::default()
    }
}

impl<R: ReadableStorageTraits, W: WritableStorageTraits> LayoutOps<R, W> for TabularLayout {
    fn into_file(&mut self, arr: Array<W>) -> StorageResult<()> {
        let mut ans = Vec::<u64>::new();
        let mut count = 0;
        // 6. We insert some data into the Array provided a certain shape. That is,
        // we are trying to create an array of a certain Shape (first vector), with
        // the provided values (second vector). What's more, an offset can be set;
        // that is, we can insert the created array with and X and Y shift. Lastly,
        // the region is written provided the aforementioned data and offset
        for (subject, triples) in self.get_graph() {
            for triple in triples {
                if ans.len() == 1024 {
                    let _ = arr
                        .store_chunk_elements(&[count, 0], ans.as_slice())
                        .unwrap();
                    ans = Vec::<u64>::new();
                    count += 1;
                }

                TabularLayout::insert_triple(&subject, triple.0, triple.1, &mut ans, &self.get_dictionary())
            }
        }

        if ans.len() > 0 {
            let _ = arr
                .store_array_subset_elements(
                    &ArraySubset::new_with_start_shape(
                        vec![count * 1024, 0],
                        vec![ans.len() as u64 / 3, 3],
                    )
                    .unwrap(),
                    ans.as_slice(),
                )
                .unwrap();
        }

        Ok(())
    }

    fn from_file(&mut self, arr: Array<R>) -> StorageResult<ZarrArray> {
        let matrix = Mutex::new(TriMat::new((
            self.get_dictionary().subjects_size(),
            self.get_dictionary().objects_size(),
        )));
        (0..arr.chunk_grid_shape().unwrap()[0] as usize)
            .par_bridge()
            .for_each(|i| {
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
