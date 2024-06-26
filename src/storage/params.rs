use std::num::NonZeroU64;

use crate::dictionary::Dictionary;
use crate::io::Graph;

pub enum Backend<'a> {
    FileSystem(&'a str),
    HTTP(&'a str),
}

pub enum Serialization {
    Zarr,
    Sparse,
}

pub enum ChunkingStrategy {
    Chunk,
    Sharding(u64),
    Best,
}

pub enum ThreadingStrategy {
    Single,
    Multi,
}

#[derive(Clone)]
pub enum ReferenceSystem {
    SPO,
    SOP,
    PSO,
    POS,
    OSP,
    OPS,
}

#[derive(Default)]
pub struct Dimensionality {
    graph_size: Option<usize>,
    pub(crate) first_term_size: usize,
    _second_term_size: usize,
    pub(crate) third_term_size: usize,
}

impl From<ChunkingStrategy> for NonZeroU64 {
    fn from(value: ChunkingStrategy) -> Self {
        match value {
            ChunkingStrategy::Chunk => NonZeroU64::new(1).unwrap(),
            ChunkingStrategy::Sharding(size) => NonZeroU64::new(size).unwrap(),
            ChunkingStrategy::Best => NonZeroU64::new(16).unwrap(), // TODO: set to the number of threads
        }
    }
}

impl AsRef<str> for ReferenceSystem {
    fn as_ref(&self) -> &str {
        match self {
            ReferenceSystem::SPO => "spo",
            ReferenceSystem::SOP => "sop",
            ReferenceSystem::PSO => "pso",
            ReferenceSystem::POS => "pos",
            ReferenceSystem::OSP => "osp",
            ReferenceSystem::OPS => "ops",
        }
    }
}

impl From<&str> for ReferenceSystem {
    fn from(value: &str) -> Self {
        match value {
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

impl Dimensionality {
    pub(crate) fn new(dictionary: &Dictionary, graph: &Graph) -> Self {
        Dimensionality {
            graph_size: graph
                .iter()
                .map(|triples| triples.len())
                .reduce(|acc, a| acc + a),
            first_term_size: match dictionary.get_reference_system() {
                ReferenceSystem::SPO | ReferenceSystem::SOP => dictionary.subjects_size(),
                ReferenceSystem::POS | ReferenceSystem::PSO => dictionary.predicates_size(),
                ReferenceSystem::OPS | ReferenceSystem::OSP => dictionary.objects_size(),
            },
            _second_term_size: match dictionary.get_reference_system() {
                ReferenceSystem::PSO | ReferenceSystem::OSP => dictionary.subjects_size(),
                ReferenceSystem::SPO | ReferenceSystem::OPS => dictionary.predicates_size(),
                ReferenceSystem::SOP | ReferenceSystem::POS => dictionary.objects_size(),
            },
            third_term_size: match dictionary.get_reference_system() {
                ReferenceSystem::POS | ReferenceSystem::OPS => dictionary.subjects_size(),
                ReferenceSystem::SOP | ReferenceSystem::OSP => dictionary.predicates_size(),
                ReferenceSystem::SPO | ReferenceSystem::PSO => dictionary.objects_size(),
            },
        }
    }

    pub(crate) fn get_graph_size(&self) -> u64 {
        self.graph_size.unwrap() as u64
    }

    pub(crate) fn get_first_term_size(&self) -> u64 {
        self.first_term_size as u64
    }

    // pub(crate) fn get_second_term_size(&self) -> u64 {
    //     self._second_term_size as u64
    // }

    pub(crate) fn get_third_term_size(&self) -> u64 {
        self.third_term_size as u64
    }
}
