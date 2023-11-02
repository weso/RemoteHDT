use clap::Parser;
use remote_hdt::remote_hdt::{RemoteHDT, RemoteHDTResult};

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
    RemoteHDT::new().serialize(&args.zarr, &args.rdf)?;
    Ok(())
}
