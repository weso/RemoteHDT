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
use zarrs::storage::store::OpendalStore;

use crate::dictionary::Dictionary;
use crate::error::RemoteHDTError;
use crate::io::Graph;
use crate::utils::columns_per_shard;
use crate::utils::rows_per_shard;
use crate::utils::value_to_term;

use super::ChunkingStrategy;
use super::Dimensionality;
use super::ReferenceSystem;


type ArrayToBytesCodec = Box<dyn ArrayToBytesCodecTraits>;

pub mod coordinates;


pub trait StructureOps<C> {
    
}

pub trait Structure<C>: StructureOps<C> {
}
