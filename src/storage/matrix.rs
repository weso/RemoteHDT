use oxigraph::model::NamedNode;
use oxigraph::model::Term;
use proc_macros::Layout;
use rayon::iter::ParallelBridge;
use rayon::prelude::ParallelIterator;
use sprs::TriMat;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use zarrs::array::codec::ArrayToBytesCodecTraits;
use zarrs::array::codec::BytesCodec;
use zarrs::array::codec::BytesToBytesCodecTraits;
use zarrs::array::codec::GzipCodec;
use zarrs::array::Array;
use zarrs::array::ChunkGrid;
use zarrs::array::DataType;
use zarrs::array::DimensionName;
use zarrs::array::FillValue;
use zarrs::storage::ReadableStorageTraits;
use zarrs::storage::WritableStorageTraits;

use super::dictionary::Dictionary;
use super::private::LayoutConfiguration;
use super::private::LayoutFields;
use super::private::LayoutOps;
use super::StorageResult;
use super::ZarrArray;

use crate::io::Graph;

#[derive(Default, Layout)]
pub struct MatrixLayout {
    dictionary: Dictionary,
    graph: Graph,
    rdf_path: String,
}

impl MatrixLayout {
    fn create_array<'a>(&self, triples: &Vec<(NamedNode, Term)>) -> Result<Vec<u8>, String> {
        let slice: Vec<AtomicU8> = vec![0u8; self.dictionary.objects_size()]
            .iter()
            .map(|&n| AtomicU8::new(n))
            .collect();

        triples.iter().for_each(|(predicate, object)| {
            let pidx = self
                .get_dictionary()
                .get_predicate_idx_unchecked(&predicate.to_string());
            let oidx = self
                .get_dictionary()
                .get_object_idx_unchecked(&object.to_string());

            slice[oidx].store(pidx as u8, Ordering::Relaxed);
        });

        Ok(slice
            .iter()
            .map(|elem| elem.load(Ordering::Relaxed))
            .collect())
    }
}

impl LayoutConfiguration for MatrixLayout {
    fn shape(&self) -> Vec<u64> {
        vec![
            self.get_dictionary().subjects_size() as u64,
            self.get_dictionary().objects_size() as u64,
        ]
    }

    fn data_type(&self) -> DataType {
        DataType::UInt8
    }

    fn chunk_shape(&self) -> ChunkGrid {
        vec![1, self.get_dictionary().objects_size() as u64].into()
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

    fn array_to_bytes_codec(&self) -> StorageResult<Box<dyn ArrayToBytesCodecTraits>> {
        Ok(Box::<BytesCodec>::default())
    }

    fn bytes_to_bytes_codec(&self) -> Vec<Box<dyn BytesToBytesCodecTraits>> {
        vec![Box::new(GzipCodec::new(5).unwrap())]
    }
}

impl<R: ReadableStorageTraits, W: WritableStorageTraits> LayoutOps<R, W> for MatrixLayout {
    fn into_file(&mut self, arr: Array<W>) -> StorageResult<()> {
        self.get_graph().iter().for_each(|(subject, triples)| {
            let i = self.get_dictionary().get_subject_idx_unchecked(&subject.to_string()) as u64;
            let chunk = self.create_array(triples).unwrap();
            let _ = arr.store_chunk_elements(&vec![i, 0], chunk.as_slice());
        });

        Ok(())
    }

    fn from_file(&mut self, arr: Array<R>) -> StorageResult<ZarrArray> {
        let matrix = Mutex::new(TriMat::new((
            self.get_dictionary().subjects_size(),
            self.get_dictionary().objects_size(),
        )));
        (0..self.get_dictionary().subjects_size())
            .par_bridge()
            .for_each(|subject_idx| {
                // Using this chunking strategy allows us to keep RAM usage low,
                // as we load elements by row
                arr.retrieve_chunk_elements::<u8>(&[subject_idx as u64, 0])
                    .unwrap()
                    .iter()
                    .enumerate()
                    .for_each(|(object_idx, &predicate_idx)| {
                        if predicate_idx != 0 {
                            matrix.lock().unwrap().add_triplet(
                                subject_idx,
                                object_idx,
                                predicate_idx,
                            );
                        }
                    })
            });

        // We use a CSC Matrix because typically, RDF knowledge graphs tend to
        // have more rows than columns
        let x = matrix.lock().unwrap();
        Ok(x.to_csc())
    }
}
