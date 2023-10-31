use clap::Parser;
use remote_hdt::remote_hdt::RemoteHDTBuilder;
use std::error::Error;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input RDF file
    #[arg(short, long)]
    rdf: String,

    /// Output Zarr directory
    #[arg(short, long, default_value = "root.zarr")]
    zarr: String,

    /// Name of the Array to be created
    #[arg(short, long, default_value = "array_name")]
    array_name: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Args = Args::parse();

    RemoteHDTBuilder::new(&args.zarr)?
        .rdf_path(&args.rdf)
        .array_name(&args.array_name)
        .build()
        .serialize()?;

    Ok(())
}
