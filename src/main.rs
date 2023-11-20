use clap::Parser;
use remote_hdt::storage::{tabular::TabularLayout, ChunkingStrategy, LocalStorage, StorageResult};

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
    LocalStorage::new(TabularLayout).serialize(&args.zarr, &args.rdf, ChunkingStrategy::Chunk)?;
    Ok(())
}
