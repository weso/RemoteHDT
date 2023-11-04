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
    PathExistsError,
    #[error(transparent)]
    GZipCompression(#[from] GzipCompressionLevelError),
}

#[derive(Error, Debug)]
pub enum EngineError {
    #[error(transparent)]
    Dimensionality(#[from] IncompatibleDimensionalityError),
    #[error(transparent)]
    Array(#[from] ArrayError),
}
