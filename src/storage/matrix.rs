use oxigraph::model::graph::Iter;
use oxigraph::model::Graph;
use oxigraph::model::TripleRef;
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

#[derive(Default, Layout)]
pub struct MatrixLayout {
    dictionary: Dictionary,
    graph: Graph,
    rdf_path: String,
}

impl MatrixLayout {
    fn create_array<'a>(&self, triples: &Vec<TripleRef<'_>>) -> Result<Vec<u8>, String> {
        let slice: Vec<AtomicU8> = vec![0; self.dictionary.objects_size()]
            .iter()
            .map(|&n| AtomicU8::new(n))
            .collect();

        triples.iter().for_each(|triple| {
            let pidx = self
                .dictionary
                .get_predicate_idx_unchecked(&triple.predicate.to_string());
            let oidx = self
                .dictionary
                .get_object_idx_unchecked(&triple.object.to_string());

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
        let mut prev_subject = None;
        let mut subject_count = 0;
        let mut triples_for_subject = Vec::new();

        self.get_graph().for_each(|triple| {
            if prev_subject.is_none() {
                prev_subject = Some(triple.subject);
            }

            if prev_subject == Some(triple.subject) {
                triples_for_subject.push(triple);
            } else {
                let _ = arr
                    .store_chunk_elements(
                        &vec![subject_count, 0],
                        self.create_array(&triples_for_subject)
                            .unwrap() // TODO: remove unwrap
                            .as_slice(),
                    )
                    .unwrap();
                subject_count += 1;
                triples_for_subject = Vec::new();
            }

            prev_subject = Some(triple.subject);
        });

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
                    .par_bridge()
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
