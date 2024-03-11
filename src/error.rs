use std::convert::Infallible;
use thiserror::Error;
use zarrs::array::codec::bytes_to_bytes::gzip::GzipCompressionLevelError;
use zarrs::array::codec::CodecError;
use zarrs::array::ArrayCreateError;
use zarrs::array::ArrayError;
use zarrs::array::NonZeroError;
use zarrs::array_subset::IncompatibleDimensionalityError;
use zarrs::array_subset::IncompatibleStartEndIndicesError;
use zarrs::group::GroupCreateError;
use zarrs::storage::store::FilesystemStoreCreateError;
use zarrs::storage::store::HTTPStoreCreateError;
use zarrs::storage::StorageError;

#[derive(Error, Debug)]
pub enum RemoteHDTError {
    #[error(transparent)]
    Dimensionality(#[from] IncompatibleDimensionalityError),
    #[error(transparent)]
    Infallible(#[from] Infallible),
    #[error(transparent)]
    FileSystemCreate(#[from] FilesystemStoreCreateError),
    #[error(transparent)]
    GroupCreate(#[from] GroupCreateError),
    #[error(transparent)]
    ArrayCreate(#[from] ArrayCreateError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Array(#[from] ArrayError),
    #[error(transparent)]
    HTTPCreate(#[from] HTTPStoreCreateError),
    #[error("The Path already exists, please provide an empty path")]
    PathExists,
    #[error("The Path does not exist, please provide another path")]
    PathDoesNotExist,
    #[error(transparent)]
    GZipCompression(#[from] GzipCompressionLevelError),
    #[error("The Graph you are trying to serialize is empty")]
    EmptyGraph,
    #[error(transparent)]
    Ops(#[from] OpsError),
    #[error("The subjects has not been serialized properly")]
    SubjectsNotInJSON,
    #[error("The predicates has not been serialized properly")]
    PredicatesNotInJSON,
    #[error("The objects has not been serialized properly")]
    ObjectsNotInJSON,
    #[error("The Reference System has not been serialized properly")]
    ReferenceSystemNotInJSON,
    #[error("Error serializing the triples of the Graph")]
    TripleSerialization,
    #[error("The provided path is not valid")]
    OsPathToString,
    #[error("The provided backend is read-only")]
    ReadOnlyBackend,
    #[error("Error while parsing the RDF graph")]
    RdfParse,
    #[error(transparent)]
    NonZero(#[from] NonZeroError),
}

#[derive(Error, Debug)]
pub enum EngineError {
    #[error(transparent)]
    Dimensionality(#[from] IncompatibleDimensionalityError),
    #[error(transparent)]
    Array(#[from] ArrayError),
    #[error("Operation error")]
    Operation,
    #[error(transparent)]
    IncompatibleStartEndIndicesError(#[from] IncompatibleStartEndIndicesError),
    #[error(transparent)]
    Codec(#[from] CodecError),
}

#[derive(Error, Debug)]
pub enum ParserError {
    #[error("Could not parse the Dicitonary: `{0}`")]
    Dictionary(String),
    #[error("Could not parse the Graph: `{0}`")]
    Graph(String),
    #[error("Format {0} not supported")]
    NotSupportedFormat(String),
    #[error("No format provided")]
    NoFormatProvided,
}

#[derive(Error, Debug)]
pub enum OpsError {
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error("The provided subject could not be found")]
    SubjectNotFound,
    #[error("The provided predicate could not be found")]
    PredicateNotFound,
    #[error("The provided object could not be found")]
    ObjectNotFound,
    #[error("The array has not been loaded correctly")]
    EmptyArray,
    #[error("The sparse array has not been loaded correctly")]
    EmptySparseArray,
}
