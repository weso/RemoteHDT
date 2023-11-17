use clap::Parser;
use remote_hdt::storage::{Layout, Storage, StorageResult};

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
    Storage::new(Layout::Tabular).serialize(&args.zarr, &args.rdf)?;
    Ok(())
}
