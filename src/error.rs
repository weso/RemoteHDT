use std::convert::Infallible;
use thiserror::Error;
use zarrs::array::codec::bytes_to_bytes::gzip::GzipCompressionLevelError;
use zarrs::array::ArrayCreateError;
use zarrs::array::ArrayError;
use zarrs::array_subset::IncompatibleDimensionalityError;
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
    #[error(transparent)]
    GZipCompression(#[from] GzipCompressionLevelError),
    #[error("The Graph you are trying to serialize is empty")]
    EmptyGraph,
}

#[derive(Error, Debug)]
pub enum EngineError {
    #[error(transparent)]
    Dimensionality(#[from] IncompatibleDimensionalityError),
    #[error(transparent)]
    Array(#[from] ArrayError),
    #[error("Operation error")]
    Operation,
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
