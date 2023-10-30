use ndarray::parallel::prelude::IndexedParallelIterator;
use ndarray::parallel::prelude::IntoParallelRefIterator;
use ndarray::parallel::prelude::ParallelIterator;
use ndarray::{ArcArray, ArcArray1, Array2, ArrayBase, Axis, Dim, Ix3, IxDynImpl, OwnedArcRepr};
use rdf_rs::RdfParser;
use sophia::api::prelude::Graph;
use sophia::api::term::CmpTerm;
use sophia::api::term::SimpleTerm;
use sophia::api::term::Term;
use sophia::api::triple::Triple;
use sophia::inmem::index::TermIndexFullError;
use sophia::term::ArcTerm;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use zarr3::codecs::bb::gzip_codec::GzipCodec;
use zarr3::prelude::smallvec::smallvec;
use zarr3::prelude::{
    create_root_group, Array, ArrayMetadataBuilder, ArrayRegion, GroupMetadata, ReadableMetadata,
};
use zarr3::store::filesystem::FileSystemStore;
use zarr3::store::{NodeKey, NodeName};
use zarr3::{ArcArrayD, CoordVec};

pub type ArcArray3 = ArcArray<u8, Ix3>;

#[derive(Default)]
pub struct Domain {
    subjects: Vec<CmpTerm<ArcTerm>>,
    predicates: Vec<CmpTerm<ArcTerm>>,
    objects: Vec<CmpTerm<ArcTerm>>,
}

pub enum DimensionName {
    Subject,
    Predicate,
    Object,
}

#[derive(PartialEq)]
pub enum ReferenceSystem {
    SPO,
    SOP,
    PSO,
    POS,
    OSP,
    OPS,
}

pub enum Field {
    Subject(usize),
    Predicate(usize),
    Object(usize),
}

pub struct RemoteHDT<'a> {
    rdf_path: &'a str,
    zarr_path: &'a str,
    array_name: &'a str,
    reference_system: ReferenceSystem,
    array: Option<ArcArray3>,
    domain: Domain,
}

pub struct RemoteHDTBuilder<'a> {
    rdf_path: &'a str,
    zarr_path: &'a str,
    array_name: &'a str,
    reference_system: ReferenceSystem,
    array: Option<ArcArray3>,
    domain: Domain,
}

pub trait ChunkEngine<'a> {
    fn get_chunk(
        &self,
        array: Array<'a, FileSystemStore, u8>,
        term: Field,
        reference_system: &ReferenceSystem,
    ) -> Result<ArcArrayD<u8>, String> {
        match term {
            Field::Subject(term) => match reference_system {
                ReferenceSystem::SPO => self.get_first_term(array, term),
                ReferenceSystem::SOP => todo!(),
                ReferenceSystem::PSO => todo!(),
                ReferenceSystem::POS => todo!(),
                ReferenceSystem::OSP => todo!(),
                ReferenceSystem::OPS => todo!(),
            },
            Field::Predicate(term) => todo!(),
            Field::Object(term) => todo!(),
        }
    }

    fn get_first_term(
        &self,
        array: Array<'a, FileSystemStore, u8>,
        term: usize,
    ) -> Result<ArcArrayD<u8>, String> {
        match array.read_chunk(&smallvec![term as u64, 0, 0]) {
            Ok(option) => match option {
                Some(arr) => Ok(arr),                 // TODO: fix this
                None => return Err(String::from("")), // TODO: fix this
            },
            Err(_) => Err(String::from("")), // TODO: fix this
        }
    }

    fn get_subject(&self, subject: usize) -> Result<ArcArrayD<u8>, String>;
    fn get_predicate(&self, predicate: usize) -> Result<ArcArray3, String>;
    fn get_object(&self, object: usize) -> Result<ArcArray3, String>;
}

pub trait Engine {
    fn get(
        &self,
        array: &Option<ArcArray3>,
        term: Field,
        reference_system: &ReferenceSystem,
    ) -> Result<ArcArray3, String> {
        let arr: ArcArray3 = match array {
            Some(arr) => arr.clone(),
            None => return Err(String::from("The array should have been loaded")),
        };

        let binding = arr.to_owned();
        let shape = binding.shape();

        let flattened: ArcArray1<u8> = match term {
            Field::Subject(term) => match reference_system {
                ReferenceSystem::SPO => self.get_first_term(arr, shape[0], term),
                ReferenceSystem::SOP => self.get_first_term(arr, shape[0], term),
                ReferenceSystem::PSO => self.get_second_term(arr, shape[1], term),
                ReferenceSystem::POS => self.get_third_term(arr, shape[2], term),
                ReferenceSystem::OSP => self.get_second_term(arr, shape[1], term),
                ReferenceSystem::OPS => self.get_third_term(arr, shape[2], term),
            },
            Field::Predicate(term) => match reference_system {
                ReferenceSystem::SPO => self.get_second_term(arr, shape[1], term),
                ReferenceSystem::SOP => self.get_third_term(arr, shape[2], term),
                ReferenceSystem::PSO => self.get_first_term(arr, shape[0], term),
                ReferenceSystem::POS => self.get_first_term(arr, shape[0], term),
                ReferenceSystem::OSP => self.get_third_term(arr, shape[2], term),
                ReferenceSystem::OPS => self.get_second_term(arr, shape[1], term),
            },
            Field::Object(term) => match reference_system {
                ReferenceSystem::SPO => self.get_third_term(arr, shape[2], term),
                ReferenceSystem::SOP => self.get_second_term(arr, shape[1], term),
                ReferenceSystem::PSO => self.get_third_term(arr, shape[2], term),
                ReferenceSystem::POS => self.get_second_term(arr, shape[1], term),
                ReferenceSystem::OSP => self.get_first_term(arr, shape[0], term),
                ReferenceSystem::OPS => self.get_first_term(arr, shape[0], term),
            },
        };

        let shaped = match flattened.into_shape(shape) {
            Ok(shaped) => shaped,
            Err(_) => return Err(String::from("Error converting to the required Shape")),
        };

        match shaped.into_dimensionality::<Ix3>() {
            Ok(ans) => Ok(ans),
            Err(_) => Err(String::from("Error assigning the dimensionality")),
        }
    }

    fn get_first_term(&self, array: ArcArray3, size: usize, term: usize) -> ArcArray1<u8> {
        array
            .axis_iter(Axis(0))
            .enumerate()
            .flat_map(|(i, two_dim_array)| {
                let factor: Array2<u8> = if i == term {
                    Array2::eye(size)
                } else {
                    Array2::zeros((size, size))
                };
                factor.dot(&two_dim_array)
            })
            .collect::<ArcArray1<u8>>()
    }

    fn get_second_term(&self, array: ArcArray3, size: usize, term: usize) -> ArcArray1<u8> {
        let mut factor: Array2<u8> = Array2::zeros((size, size));
        factor[[term, term]] = 1; // we place it in the main diagonal

        array
            .axis_iter(Axis(0))
            .flat_map(|two_dim_array| factor.dot(&two_dim_array))
            .collect::<ArcArray1<u8>>()
    }

    fn get_third_term(&self, array: ArcArray3, size: usize, term: usize) -> ArcArray1<u8> {
        let mut factor: Array2<u8> = Array2::zeros((size, size));
        factor[[term, term]] = 1; // we place it in the main diagonal

        array
            .axis_iter(Axis(0))
            .flat_map(|two_dim_array| two_dim_array.dot(&factor))
            .collect::<ArcArray1<u8>>()
    }

    fn get_subject(&self, subject: usize) -> Result<ArcArray3, String>;
    fn get_predicate(&self, predicate: usize) -> Result<ArcArray3, String>;
    fn get_object(&self, object: usize) -> Result<ArcArray3, String>;
}

impl From<DimensionName> for Option<String> {
    fn from(value: DimensionName) -> Self {
        match value {
            DimensionName::Subject => Some("Subject".to_string()),
            DimensionName::Predicate => Some("Predicate".to_string()),
            DimensionName::Object => Some("Object".to_string()),
        }
    }
}

impl From<&ReferenceSystem> for String {
    fn from(value: &ReferenceSystem) -> Self {
        match value {
            ReferenceSystem::SPO => String::from("spo"),
            ReferenceSystem::SOP => String::from("sop"),
            ReferenceSystem::PSO => String::from("pso"),
            ReferenceSystem::POS => String::from("pos"),
            ReferenceSystem::OSP => String::from("osp"),
            ReferenceSystem::OPS => String::from("ops"),
        }
    }
}

impl From<String> for ReferenceSystem {
    fn from(value: String) -> Self {
        match value.as_str() {
            "spo" => ReferenceSystem::SPO,
            "sop" => ReferenceSystem::SOP,
            "pso" => ReferenceSystem::PSO,
            "pos" => ReferenceSystem::POS,
            "osp" => ReferenceSystem::OSP,
            "ops" => ReferenceSystem::OPS,
            _ => ReferenceSystem::SPO,
        }
    }
}

impl Domain {
    pub fn get_subject(&self, subject: &str) -> Option<usize> {
        self.subjects
            .par_iter()
            .position_first(|e| e.to_owned() == subject.into_term::<CmpTerm<ArcTerm>>())
    }

    pub fn get_predicate(&self, predicate: &str) -> Option<usize> {
        self.subjects
            .par_iter()
            .position_first(|e| e.to_owned() == predicate.into_term::<CmpTerm<ArcTerm>>())
    }

    pub fn get_object(&self, object: &str) -> Option<usize> {
        self.subjects
            .par_iter()
            .position_first(|e| e.to_owned() == object.into_term::<CmpTerm<ArcTerm>>())
    }
}

impl ReferenceSystem {
    fn shape(&self, domain: &Domain) -> [usize; 3] {
        let shape = match self {
            ReferenceSystem::SPO => [&domain.subjects, &domain.predicates, &domain.objects],
            ReferenceSystem::SOP => [&domain.subjects, &domain.objects, &domain.predicates],
            ReferenceSystem::PSO => [&domain.predicates, &domain.subjects, &domain.objects],
            ReferenceSystem::POS => [&domain.predicates, &domain.objects, &domain.subjects],
            ReferenceSystem::OSP => [&domain.objects, &domain.subjects, &domain.predicates],
            ReferenceSystem::OPS => [&domain.objects, &domain.predicates, &domain.subjects],
        }
        .iter()
        .map(|dimension| dimension.len())
        .collect::<Vec<usize>>();

        [shape[0], shape[1], shape[2]]
    }

    fn shape_u64(&self, domain: &Domain) -> [u64; 3] {
        let shape = self
            .shape(domain)
            .into_iter()
            .map(|dimension| dimension as u64)
            .collect::<Vec<u64>>();

        [shape[0], shape[1], shape[2]]
    }

    fn dimension_names(&self) -> CoordVec<Option<String>> {
        match self {
            ReferenceSystem::SPO => smallvec![
                DimensionName::Subject.into(),
                DimensionName::Predicate.into(),
                DimensionName::Object.into()
            ],
            ReferenceSystem::SOP => smallvec![
                DimensionName::Subject.into(),
                DimensionName::Object.into(),
                DimensionName::Predicate.into(),
            ],
            ReferenceSystem::PSO => smallvec![
                DimensionName::Predicate.into(),
                DimensionName::Subject.into(),
                DimensionName::Object.into()
            ],
            ReferenceSystem::POS => smallvec![
                DimensionName::Predicate.into(),
                DimensionName::Object.into(),
                DimensionName::Subject.into(),
            ],
            ReferenceSystem::OSP => smallvec![
                DimensionName::Object.into(),
                DimensionName::Subject.into(),
                DimensionName::Predicate.into(),
            ],
            ReferenceSystem::OPS => smallvec![
                DimensionName::Object.into(),
                DimensionName::Predicate.into(),
                DimensionName::Subject.into(),
            ],
        }
    }

    fn vec_size(&self, domain: &Domain) -> usize {
        match self {
            ReferenceSystem::SPO => &domain.predicates.len() * &domain.objects.len(),
            ReferenceSystem::SOP => &domain.predicates.len() * &domain.objects.len(),
            ReferenceSystem::PSO => &domain.subjects.len() * &domain.objects.len(),
            ReferenceSystem::POS => &domain.subjects.len() * &domain.objects.len(),
            ReferenceSystem::OSP => &domain.subjects.len() * &domain.predicates.len(),
            ReferenceSystem::OPS => &domain.subjects.len() * &domain.predicates.len(),
        }
    }

    fn chunk_size(&self, domain: &Domain) -> [usize; 3] {
        let domain = self.shape(domain);
        [1, domain[1], domain[2]]
    }

    fn chunk_size_u64(&self, domain: &Domain) -> [u64; 3] {
        let domain: [u64; 3] = self.shape_u64(domain);
        [1, domain[1], domain[2]]
    }

    fn convert_to(
        &self,
        other: &ReferenceSystem,
        mut array: ArcArray3,
        domain: &Domain,
    ) -> ArcArray3 {
        if self != other {
            // In case the reference system used for serializing the Array is
            // not the same as the one selected by the user when building this
            // struct, we have to reshape the array so it holds to the user's
            // desired mechanism
            let mut v = Vec::<(usize, usize, usize, u8)>::new();

            // Could this be improved using a multithreading approach? If we use
            // rayon the solution would be possibly faster and the implementation
            // details wouldn't vary as much
            // TODO: improve this if possible
            for (i, outer) in array.outer_iter().enumerate() {
                for j in 0..outer.shape()[0] {
                    for k in 0..outer.shape()[1] {
                        // We convert the reference system used in the serialization
                        // format into SPO
                        v.push(match self {
                            ReferenceSystem::SPO => (i, j, k, outer[[j, k]]),
                            ReferenceSystem::SOP => (i, k, j, outer[[j, k]]),
                            ReferenceSystem::PSO => (j, i, k, outer[[j, k]]),
                            ReferenceSystem::POS => (j, k, i, outer[[j, k]]),
                            ReferenceSystem::OSP => (k, i, j, outer[[j, k]]),
                            ReferenceSystem::OPS => (k, j, i, outer[[j, k]]),
                        })
                    }
                }
            }

            let mut reshaped_array = ArcArray3::zeros(other.shape(domain));

            // Same here... Using rayon would be desirable
            for (s, p, o, value) in v {
                match other {
                    ReferenceSystem::SPO => reshaped_array[[s, p, o]] = value,
                    ReferenceSystem::SOP => reshaped_array[[s, o, p]] = value,
                    ReferenceSystem::PSO => reshaped_array[[p, s, o]] = value,
                    ReferenceSystem::POS => reshaped_array[[p, o, s]] = value,
                    ReferenceSystem::OSP => reshaped_array[[o, s, p]] = value,
                    ReferenceSystem::OPS => reshaped_array[[o, p, s]] = value,
                }
            }

            array = reshaped_array;
        }

        array
    }

    fn index(&self, sidx: usize, pidx: usize, oidx: usize, domain: &Domain) -> usize {
        let shape = self.shape(domain);

        let sidx = match self {
            ReferenceSystem::SPO => 0,
            ReferenceSystem::SOP => 0,
            ReferenceSystem::PSO => sidx * shape[2],
            ReferenceSystem::POS => sidx,
            ReferenceSystem::OSP => sidx * shape[2],
            ReferenceSystem::OPS => sidx,
        };

        let pidx = match self {
            ReferenceSystem::SPO => pidx * shape[2],
            ReferenceSystem::SOP => pidx,
            ReferenceSystem::PSO => 0,
            ReferenceSystem::POS => 0,
            ReferenceSystem::OSP => pidx,
            ReferenceSystem::OPS => pidx * shape[2],
        };

        let oidx = match self {
            ReferenceSystem::SPO => oidx,
            ReferenceSystem::SOP => oidx * shape[2],
            ReferenceSystem::PSO => oidx,
            ReferenceSystem::POS => oidx * shape[2],
            ReferenceSystem::OSP => 0,
            ReferenceSystem::OPS => 0,
        };

        sidx + pidx + oidx
    }
}

impl<'a> RemoteHDTBuilder<'a> {
    pub fn new(zarr_path: &'a str) -> Self {
        // Set the minimally required fields of RemoteHDT
        RemoteHDTBuilder {
            rdf_path: Default::default(),
            zarr_path,
            array_name: "array",
            reference_system: ReferenceSystem::SPO,
            array: None,
            domain: Default::default(),
        }
    }

    pub fn rdf_path(mut self, rdf_path: &'a str) -> Self {
        // Set the RDF path for it to be serialized
        self.rdf_path = rdf_path;
        self
    }

    pub fn array_name(mut self, array_name: &'a str) -> Self {
        // Set the name of the array, and return the builder by value
        self.array_name = array_name;
        self
    }

    pub fn reference_system(mut self, reference_system: ReferenceSystem) -> Self {
        // Set the system of reference, and return the builder by value
        self.reference_system = reference_system;
        self
    }

    pub fn build(self) -> RemoteHDT<'a> {
        RemoteHDT {
            rdf_path: self.rdf_path,
            zarr_path: self.zarr_path,
            array_name: self.array_name,
            reference_system: self.reference_system,
            array: self.array,
            domain: self.domain,
        }
    }
}

impl<'a> RemoteHDT<'a> {
    pub fn serialize(mut self) -> Result<Self, String> {
        // 1. First, we open the File System for us to store the ZARR project
        let path = match PathBuf::from_str(self.zarr_path) {
            Ok(path) => path,
            Err(_) => return Err(String::from("Error opening the Path for the ZARR project")),
        };

        let store = match FileSystemStore::create(path, true) {
            Ok(store) => store,
            Err(_) => return Err(String::from("Error creating the File System Store")),
        };

        // 2. We include the default Metadata to the ZARR project
        let root_group = match create_root_group(&store, GroupMetadata::default()) {
            Ok(root_group) => root_group,
            Err(_) => return Err(String::from("Error creating the Group")),
        };

        // 3. Import the RDF dump using `rdf-rs`
        let dump = RdfParser::new(self.rdf_path)?;

        let subject_binding = dump.graph.subjects();
        let predicate_binding = dump.graph.predicates();
        let object_binding = dump.graph.objects();

        self.domain = Domain {
            // We store the subjects to be serialized
            subjects: {
                let mut ans = Vec::<CmpTerm<ArcTerm>>::new();
                subject_binding.for_each(|subject| {
                    if subject.is_ok()
                        && !ans.contains(&subject.unwrap().into_term::<CmpTerm<ArcTerm>>())
                    {
                        ans.push(subject.unwrap().into_term());
                    }
                });
                ans
            },
            // We store the predicates to be serialized
            predicates: {
                let mut ans = Vec::<CmpTerm<ArcTerm>>::new();
                predicate_binding.for_each(|predicate| {
                    if predicate.is_ok()
                        && !ans.contains(&predicate.unwrap().into_term::<CmpTerm<ArcTerm>>())
                    {
                        ans.push(predicate.unwrap().into_term());
                    }
                });
                ans
            },
            // We store the objects to be serialized
            objects: {
                let mut ans = Vec::<CmpTerm<ArcTerm>>::new();
                object_binding.for_each(|object| {
                    if object.is_ok()
                        && !ans.contains(&object.unwrap().into_term::<CmpTerm<ArcTerm>>())
                    {
                        ans.push(object.unwrap().into_term());
                    }
                });
                ans
            },
        };

        // 4. Build the structure of the Array; as such, several parameters of it are
        // tweaked. Namely, the size of the array, the size of the chunks, the name
        // of the different dimensions and the default values
        let arr_meta =
            ArrayMetadataBuilder::<u8>::new(&self.reference_system.shape_u64(&self.domain))
                .dimension_names(self.reference_system.dimension_names())?
                .chunk_grid(
                    self.reference_system
                        .chunk_size_u64(&self.domain)
                        .as_slice(),
                )?
                .push_bb_codec(GzipCodec::default())
                .set_attribute(
                    "subjects".to_string(),
                    &self
                        .domain
                        .subjects
                        .par_iter()
                        .map(|subject| format!("{:?}", &subject.0.iri().unwrap()))
                        .collect::<Vec<_>>(),
                )?
                .set_attribute(
                    "predicates".to_string(),
                    &self
                        .domain
                        .predicates
                        .par_iter()
                        .map(|predicate| format!("{:?}", predicate))
                        .collect::<Vec<_>>(),
                )?
                .set_attribute(
                    "objects".to_string(),
                    &self
                        .domain
                        .objects
                        .par_iter()
                        .map(
                            |object: &CmpTerm<sophia::term::GenericTerm<std::sync::Arc<str>>>| {
                                format!("{:?}", object)
                            },
                        )
                        .collect::<Vec<_>>(),
                )?
                .set_attribute(
                    "reference_system".to_string(),
                    String::from(&self.reference_system),
                )?
                .build();

        // 5. Create the Array given the name of it
        let node_name = match self.array_name.parse::<NodeName>() {
            Ok(node_name) => node_name,
            Err(_) => return Err(String::from("Error parsing the NodeName")),
        };

        let arr = match root_group.create_array::<u8>(node_name, arr_meta) {
            Ok(array) => array,
            Err(_) => return Err(String::from("Error creating the Array")),
        };

        // 6. We insert some data into the Array provided a certain shape. That is,
        // we are trying to create an array of a certain Shape (first vector), with
        // the provided values (second vector). What's more, an offset can be set;
        // that is, we can insert the created array with and X and Y shift. Lastly,
        // the region is written provided the aforementioned data and offset
        match self.reference_system {
            ReferenceSystem::SPO | ReferenceSystem::SOP => self.domain.subjects.to_owned(),
            ReferenceSystem::PSO | ReferenceSystem::POS => self.domain.predicates.to_owned(),
            ReferenceSystem::OSP | ReferenceSystem::OPS => self.domain.objects.to_owned(),
        }
        .par_iter()
        .enumerate()
        .for_each(|(i, term)| {
            let _ = arr.write_chunk(&smallvec![i as u64, 0, 0], {
                self.create_array(
                    dump.graph
                        .triples()
                        .filter(|triple| {
                            term == match self.reference_system {
                                ReferenceSystem::SPO | ReferenceSystem::SOP => triple.unwrap().s(),
                                ReferenceSystem::PSO | ReferenceSystem::POS => triple.unwrap().p(),
                                ReferenceSystem::OSP | ReferenceSystem::OPS => triple.unwrap().o(),
                            }
                        })
                        .collect::<Vec<Result<[&SimpleTerm<'static>; 3], TermIndexFullError>>>(),
                    i,
                )
                .unwrap()
            });
        });

        Ok(self)
    }

    fn create_array(
        &self,
        triples: Vec<Result<[&SimpleTerm<'static>; 3], TermIndexFullError>>,
        idx: usize,
    ) -> Result<ArrayBase<OwnedArcRepr<u8>, Dim<IxDynImpl>>, String> {
        match ArcArrayD::from_shape_vec(
            self.reference_system.chunk_size(&self.domain).as_slice(),
            {
                let slice: Vec<AtomicU8> = vec![0u8; self.reference_system.vec_size(&self.domain)]
                    .par_iter()
                    .map(|&n| AtomicU8::new(n))
                    .collect();

                triples.par_iter().for_each(|triple| {
                    let sidx = match self.reference_system {
                        ReferenceSystem::SPO | ReferenceSystem::SOP => idx,
                        _ => match self
                            .domain
                            .subjects
                            .par_iter()
                            .position_first(|elem| elem == triple.unwrap().s())
                        {
                            Some(sidx) => sidx,
                            None => return,
                        },
                    };
                    let pidx = match self.reference_system {
                        ReferenceSystem::PSO | ReferenceSystem::POS => idx,
                        _ => match self
                            .domain
                            .predicates
                            .par_iter()
                            .position_first(|elem| elem == triple.unwrap().p())
                        {
                            Some(pidx) => pidx,
                            None => return,
                        },
                    };
                    let oidx = match self.reference_system {
                        ReferenceSystem::OSP | ReferenceSystem::OPS => idx,
                        _ => match self
                            .domain
                            .objects
                            .par_iter()
                            .position_first(|elem| elem == triple.unwrap().o())
                        {
                            Some(oidx) => oidx,
                            None => return,
                        },
                    };

                    slice[self.reference_system.index(sidx, pidx, oidx, &self.domain)]
                        .store(1u8, Ordering::Relaxed);
                });

                slice
                    .par_iter()
                    .map(|elem| elem.load(Ordering::Relaxed))
                    .collect::<Vec<u8>>()
            },
        ) {
            Ok(data) => Ok(data),
            Err(_) => return Err(String::from("Error creating the array")),
        }
    }

    pub fn parse(mut self) -> Result<Self, String> {
        // 1. First, we open the File System for us to retrieve the ZARR array
        let path = match PathBuf::from_str(self.zarr_path) {
            Ok(path) => path,
            Err(_) => return Err(String::from("Error opening the Path for the ZARR project")),
        };

        let store = match FileSystemStore::open(path) {
            Ok(store) => store,
            Err(_) => return Err(String::from("Error opening the File System Store")),
        };

        // 2. We create the NodeKey from the ArrayName
        let key = match NodeKey::from_str(self.array_name) {
            Ok(key) => key,
            Err(_) => return Err(String::from("Error creating NodeKey from the ArrayName")),
        };

        // 3. We import the Array from the FileSystemStore that we have created
        let arr: Array<'_, FileSystemStore, u8> = match Array::from_store(&store, key) {
            Ok(arr) => arr,
            Err(_) => return Err(String::from("Error importing Array from store")),
        };

        // 4. We get the attributes so we can obtain some values that we will need
        let attributes = arr.get_attributes();

        self.domain = Domain {
            subjects: attributes
                .get("subjects")
                .unwrap()
                .as_array()
                .unwrap()
                .par_iter()
                .map(|subject| subject.as_str().unwrap().into_term())
                .collect::<Vec<CmpTerm<ArcTerm>>>(),
            predicates: attributes
                .get("predicates")
                .unwrap()
                .as_array()
                .unwrap()
                .par_iter()
                .map(|predicate| predicate.as_str().unwrap().into_term())
                .collect::<Vec<CmpTerm<ArcTerm>>>(),
            objects: attributes
                .get("objects")
                .unwrap()
                .as_array()
                .unwrap()
                .par_iter()
                .map(|object| object.as_str().unwrap().into_term())
                .collect::<Vec<CmpTerm<ArcTerm>>>(),
        };

        let reference_system = ReferenceSystem::from(
            attributes
                .get("reference_system")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string(),
        );

        // 5. We read the region from the Array we have just created
        // TODO: restrict ourselves to a certain region, not to the whole dump :(
        let shape_u64 = &reference_system.shape_u64(&self.domain);
        let array_region = ArrayRegion::from_offset_shape(&[0, 0, 0], shape_u64);

        let region = match arr.read_region(array_region) {
            Ok(region) => region,
            Err(_) => return Err(String::from("Error loading the array")),
        };

        let array = match region {
            Some(array) => array.into_dimensionality::<Ix3>(),
            None => return Err(String::from("Error loading the array")),
        };

        self.array = match array {
            Ok(ans) => Some(reference_system.convert_to(&self.reference_system, ans, &self.domain)),
            Err(_) => return Err(String::from("Error converting to a 3-dimensional array")),
        };

        Ok(self)
    }

    pub fn get_array(self) -> Result<ArcArray3, String> {
        match self.array {
            Some(array) => Ok(array),
            None => Err(String::from("Array is None")),
        }
    }

    pub fn get_domain(self) -> Domain {
        self.domain
    }
}

impl Engine for RemoteHDT<'_> {
    fn get_subject(&self, index: usize) -> Result<ArcArray3, String> {
        self.get(&self.array, Field::Subject(index), &self.reference_system)
    }

    // TODO: the current implementation works for SELECT *, but what if we SELECT predicate?
    fn get_predicate(&self, index: usize) -> Result<ArcArray3, String> {
        self.get(&self.array, Field::Predicate(index), &self.reference_system)
    }

    fn get_object(&self, index: usize) -> Result<ArcArray3, String> {
        self.get(&self.array, Field::Object(index), &self.reference_system)
    }
}

impl ChunkEngine<'_> for RemoteHDT<'_> {
    fn get_subject(&self, index: usize) -> Result<ArcArrayD<u8>, String> {
        // 1. First, we open the File System for us to retrieve the ZARR array
        let path = match PathBuf::from_str(self.zarr_path) {
            Ok(path) => path,
            Err(_) => return Err(String::from("Error opening the Path for the ZARR project")),
        };

        let store = match FileSystemStore::open(path) {
            Ok(store) => store,
            Err(_) => return Err(String::from("Error opening the File System Store")),
        };

        // 2. We create the NodeKey from the ArrayName
        let key = match NodeKey::from_str(self.array_name) {
            Ok(key) => key,
            Err(_) => return Err(String::from("Error creating NodeKey from the ArrayName")),
        };

        // 3. We import the Array from the FileSystemStore that we have created
        let arr: Array<'_, FileSystemStore, u8> = match Array::from_store(&store, key) {
            Ok(arr) => arr,
            Err(_) => return Err(String::from("Error importing Array from store")),
        };

        self.get_chunk(arr, Field::Subject(index), &self.reference_system)
    }

    // TODO: the current implementation works for SELECT *, but what if we SELECT predicate?
    fn get_predicate(&self, index: usize) -> Result<ArcArray3, String> {
        todo!()
    }

    fn get_object(&self, index: usize) -> Result<ArcArray3, String> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::{ArcArray3, Domain, Engine, ReferenceSystem, RemoteHDT};
    use sophia::{
        api::term::{CmpTerm, Term},
        term::ArcTerm,
    };

    // We create a SPO array with the following Shape:
    // [       O1  O2  03
    //    P1 [[ 1,  0,  0],
    //    P2  [ 0,  1,  0]], // S1
    //
    //         O1  O2  03
    //    P1 [[ 1,  1,  0],
    //    P2  [ 0,  0,  0]], // S2
    // ]  (Shape: Subjects = 2, Predicates = 2, Objects = 3)
    fn spo_array<'a>() -> (ArcArray3, Domain) {
        (
            ArcArray3::from_shape_vec((2, 2, 3), vec![1, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0]).unwrap(),
            Domain {
                subjects: vec![
                    "S1".into_term::<CmpTerm<ArcTerm>>(),
                    "S2".into_term::<CmpTerm<ArcTerm>>(),
                ],
                predicates: vec![
                    "P1".into_term::<CmpTerm<ArcTerm>>(),
                    "P2".into_term::<CmpTerm<ArcTerm>>(),
                ],
                objects: vec![
                    "O1".into_term::<CmpTerm<ArcTerm>>(),
                    "O2".into_term::<CmpTerm<ArcTerm>>(),
                    "O3".into_term::<CmpTerm<ArcTerm>>(),
                ],
            },
        )
    }

    #[test]
    fn convert_from_spo_2_pso_test() {
        let (array, domain) = spo_array();
        assert_eq!(
            ReferenceSystem::SPO.convert_to(&ReferenceSystem::PSO, array, &domain), // actual
            ArcArray3::from_shape_vec((2, 2, 3), vec![1, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0]).unwrap() // expected
        )
    }

    #[test]
    fn get_subject_test() {
        assert_eq!(
            RemoteHDT {
                rdf_path: Default::default(),
                zarr_path: Default::default(),
                array_name: Default::default(),
                reference_system: ReferenceSystem::SPO,
                array: Some(spo_array().0),
                domain: spo_array().1,
            }
            .get_subject(0)
            .unwrap(),
            ArcArray3::from_shape_vec((2, 2, 3), vec![1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]).unwrap()
        )
    }

    #[test]
    fn get_predicate_test() {
        assert_eq!(
            RemoteHDT {
                rdf_path: Default::default(),
                zarr_path: Default::default(),
                array_name: Default::default(),
                reference_system: ReferenceSystem::SPO,
                array: Some(spo_array().0),
                domain: spo_array().1,
            }
            .get_predicate(0)
            .unwrap(),
            ArcArray3::from_shape_vec((2, 2, 3), vec![1, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0]).unwrap()
        )
    }

    #[test]
    fn get_object_test() {
        assert_eq!(
            RemoteHDT {
                rdf_path: Default::default(),
                zarr_path: Default::default(),
                array_name: Default::default(),
                reference_system: ReferenceSystem::SPO,
                array: Some(spo_array().0),
                domain: spo_array().1,
            }
            .get_object(0)
            .unwrap(),
            ArcArray3::from_shape_vec((2, 2, 3), vec![1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0]).unwrap()
        )
    }
}
