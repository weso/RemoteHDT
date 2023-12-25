use clap::Parser;
use remote_hdt::storage::params::ChunkingStrategy;
use remote_hdt::storage::params::ReferenceSystem;
use remote_hdt::storage::params::Serialization;
use remote_hdt::storage::tabular::TabularLayout;
use remote_hdt::storage::LocalStorage;
use remote_hdt::storage::StorageResult;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input RDF file
    #[arg(short, long)]
    rdf: String,

    /// Output Zarr directory
    #[arg(short, long, default_value = "root.zarr")]
    zarr: String,
}

fn main() -> StorageResult<()> {
    let args: Args = Args::parse();
    LocalStorage::new(TabularLayout, Serialization::Sparse).serialize(
        &args.zarr,
        &args.rdf,
        ChunkingStrategy::Chunk,
        ReferenceSystem::SPO,
    )?;
    Ok(())
}
