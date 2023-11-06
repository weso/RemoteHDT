use nalgebra_sparse::CooMatrix;
use nalgebra_sparse::CsrMatrix;
use rayon::iter::ParallelBridge;
use rayon::prelude::IntoParallelRefIterator;
use rayon::prelude::ParallelIterator;
use rdf_rs::RdfParser;
use serde_json::Map;
use sophia::graph::inmem::sync::FastGraph;
use sophia::graph::GTripleSource;
use sophia::graph::Graph;
use sophia::triple::Triple;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use zarrs::array::codec::array_to_bytes::sharding::ShardingCodecBuilder;
use zarrs::array::codec::GzipCodec;
use zarrs::array::Array;
use zarrs::array::ArrayBuilder;
use zarrs::array::DataType;
use zarrs::array::DimensionName;
use zarrs::array::FillValue;
use zarrs::array_subset::ArraySubset;
use zarrs::group::GroupBuilder;
use zarrs::storage::store::FilesystemStore;
use zarrs::storage::store::HTTPStore;
use zarrs::storage::ReadableStorageTraits;

use crate::dictionary::Dictionary;
use crate::error::RemoteHDTError;
use crate::utils::term_to_value;
use crate::utils::value_to_term;

pub type ZarrArray = CsrMatrix<u8>;
pub type RemoteHDTResult<T> = Result<T, RemoteHDTError>;

const ARRAY_NAME: &str = "/group/RemoteHDT";

pub struct RemoteHDT {
    pub dictionary: Dictionary,
}

impl RemoteHDT {
    pub fn new() -> Self {
        RemoteHDT {
            dictionary: Dictionary::default(),
        }
    }

    /// # Errors
    /// Returns [`PathExistsError`] if the provided path already exists; that is,
    /// the user is trying to store the RDF dataset in an occupied storage. This
    /// is due to the fact that the user may incur in an undefined state.
    pub fn serialize<'a>(
        &mut self,
        zarr_path: &'a str,
        rdf_path: &'a str,
    ) -> RemoteHDTResult<&Self> {
        // 1. The first thing that should be done is to check whether the path
        // in which we are trying to store the dump already exists or not. If it
        // does, we should stop the execution, preventing the user from losing
        // data. Otherwise we can resume it and begin the actual proccess...
        let path = PathBuf::from_str(zarr_path)?;
        if path.exists() {
            // the actual check occurs here !!!
            return Err(RemoteHDTError::PathExistsError);
        }

        // 2. We can create the FileSystemStore appropiately
        let store = Arc::new(FilesystemStore::new(path)?);

        // Create a group and write metadata to filesystem
        let group = GroupBuilder::new().build(store.clone(), "/group")?;
        group.store_metadata()?;

        // 3. Import the RDF dump using `rdf-rs`
        let graph: Arc<FastGraph> = Arc::new(RdfParser::new(rdf_path).unwrap().graph);
        self.dictionary =
            Dictionary::from_set_terms(graph.subjects()?, graph.predicates()?, graph.objects()?);

        // 4. Build the structure of the Array; as such, several parameters of it are
        // tweaked. Namely, the size of the array, the size of the chunks, the name
        // of the different dimensions and the default values
        let shard_x = if self.dictionary.subjects_size() > 10240 {
            self.dictionary.subjects_size() / 10240
        } else {
            1
        };
        let shard_shape = vec![shard_x as u64, self.dictionary.objects_size() as u64];
        let inner_chunk_shape = vec![1, self.dictionary.objects_size() as u64];
        let mut sharding_codec_builder = ShardingCodecBuilder::new(inner_chunk_shape.clone());
        sharding_codec_builder.bytes_to_bytes_codecs(vec![Box::new(GzipCodec::new(5)?)]);
        let array = ArrayBuilder::new(
            vec![
                self.dictionary.subjects_size() as u64,
                self.dictionary.objects_size() as u64,
            ],
            DataType::UInt8,
            shard_shape.into(),
            FillValue::from(0u8),
        )
        .dimension_names(Some(vec![
            DimensionName::new("Subject"),
            DimensionName::new("Object"),
        ]))
        .array_to_bytes_codec(Box::new(sharding_codec_builder.build()))
        .attributes({
            let mut attributes = Map::new();
            attributes.insert(
                "subjects".to_string(),
                term_to_value(self.dictionary.subjects()),
            );
            attributes.insert(
                "predicates".to_string(),
                term_to_value(self.dictionary.predicates()),
            );
            attributes.insert(
                "objects".to_string(),
                term_to_value(self.dictionary.objects()),
            );
            attributes
        })
        .build(store, ARRAY_NAME)?;

        array.store_metadata()?;

        // 6. We insert some data into the Array provided a certain shape. That is,
        // we are trying to create an array of a certain Shape (first vector), with
        // the provided values (second vector). What's more, an offset can be set;
        // that is, we can insert the created array with and X and Y shift. Lastly,
        // the region is written provided the aforementioned data and offset
        let binding = graph.subjects()?;
        let mut subjects = binding.par_iter().collect::<Vec<_>>();
        subjects.sort();
        subjects
            .chunks(shard_x)
            .enumerate()
            .par_bridge()
            .for_each(|(i, chunk)| {
                let mut ans = Vec::<u8>::new();

                chunk.iter().for_each(|&subject| {
                    ans.append(&mut self.create_array(graph.triples_with_s(subject)).unwrap());
                });

                if array.shape()[0] % shard_x as u64 != 0
                    && i == (array.chunk_grid_shape().unwrap()[0] - 1) as usize
                {
                    let _ = array
                        .store_array_subset_elements(
                            &ArraySubset::new_with_start_shape(
                                vec![(i * shard_x + 1) as u64, 0],
                                vec![chunk.len() as u64, array.shape()[1]],
                            )
                            .unwrap(),
                            ans.as_slice(),
                        )
                        .unwrap();
                } else {
                    let _ = array
                        .store_chunk_elements(&[i as u64, 0], ans.as_slice())
                        .unwrap();
                }
            });

        Ok(self)
    }

    fn create_array(&self, triples: GTripleSource<FastGraph>) -> RemoteHDTResult<Vec<u8>> {
        let slice: Vec<AtomicU8> = vec![0; self.dictionary.objects_size()]
            .iter()
            .map(|&n| AtomicU8::new(n))
            .collect();

        triples.for_each(|result_triple| match result_triple {
            Ok(triple) => {
                let pidx = self
                    .dictionary
                    .get_predicate_idx(&triple.p().to_string())
                    .unwrap();
                let oidx = self
                    .dictionary
                    .get_object_idx(&triple.o().to_string())
                    .unwrap();
                slice[oidx].store(pidx as u8, Ordering::Relaxed);
            }
            Err(_) => return,
        });

        Ok(slice
            .iter()
            .map(|elem| elem.load(Ordering::Relaxed))
            .collect())
    }

    pub fn load<'a>(&mut self, zarr_path: &'a str) -> RemoteHDTResult<ZarrArray> {
        let store = Arc::new(FilesystemStore::new(zarr_path)?);
        let arr = Array::new(store, ARRAY_NAME)?;
        self.retrieve_data(arr)
    }

    pub fn connect<'a>(&mut self, url: &'a str) -> RemoteHDTResult<ZarrArray> {
        let store = Arc::new(HTTPStore::new(url)?);
        let arr = Array::new(store, ARRAY_NAME)?;
        self.retrieve_data(arr)
    }

    fn retrieve_data<T: ReadableStorageTraits + ?Sized>(
        &mut self,
        arr: Array<T>,
    ) -> RemoteHDTResult<ZarrArray> {
        // 4. We get the attributes so we can obtain some values that we will need
        let attributes = arr.attributes();

        let subjects = value_to_term(attributes.get("subjects").unwrap());
        let predicates = value_to_term(attributes.get("predicates").unwrap());
        let objects = value_to_term(attributes.get("objects").unwrap());

        self.dictionary = Dictionary::from_vec_str(subjects, predicates, objects);

        // 5. We read the region from the Array we have just created
        let matrix = Arc::new(Mutex::new(CooMatrix::<u8>::new(
            arr.shape()[0] as usize,
            arr.shape()[1] as usize,
        )));

        let number_of_chunks = arr.chunk_grid_shape().unwrap()[0] as usize;
        (0..number_of_chunks)
            .collect::<Vec<usize>>()
            .par_iter()
            .for_each(|&i| {
                // Using this chunking strategy allows us to keep RAM usage low,
                // as we load elements by row
                arr.retrieve_chunk_elements::<u8>(&[i as u64, 0])
                    .unwrap()
                    .iter()
                    .enumerate()
                    .for_each(|(j, &value)| {
                        if value != 0u8 {
                            matrix.lock().unwrap().push(
                                j / self.dictionary.objects_size()
                                    + (i as usize * self.dictionary.subjects_size()
                                        / number_of_chunks),
                                j % self.dictionary.objects_size(),
                                value,
                            );
                        }
                    })
            });

        let x = matrix.lock().unwrap().to_owned();

        Ok(CsrMatrix::from(&x))
    }
}
