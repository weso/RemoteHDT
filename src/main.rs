use clap::Parser;
use remote_hdt::remote_hdt::{RemoteHDTBuilder, RemoteHDTResult};

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

fn main() -> RemoteHDTResult<()> {
    let args: Args = Args::parse();

    RemoteHDTBuilder::new(&args.zarr)?
        .rdf_path(&args.rdf)
        .build()
        .serialize()?;

    Ok(())
}
