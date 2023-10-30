use clap::{ArgGroup, Parser};
use remote_hdt::remote_hdt::RemoteHDTBuilder;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[clap(group(
    ArgGroup::new("operation")
        .required(true)
        .args(&["serialize", "parse"]),
))]
struct Args {
    /// Input RDF file
    #[arg(short, long)]
    rdf: Option<String>,

    /// Output Zarr directory
    #[arg(short, long, default_value = "root.zarr")]
    zarr: String,

    /// Name of the Array to be created
    #[arg(short, long, default_value = "array_name")]
    array_name: String,

    /// If the user wants to serialize the given RDF ddtaset
    #[arg(long)]
    serialize: bool,

    /// If the user wants to parse the given Zarr directory
    #[arg(long)]
    parse: bool,
}

fn main() -> Result<(), String> {
    let args: Args = Args::parse();

    let (serialize, parse) = (args.serialize, args.parse);

    match (serialize, parse) {
        (true, _) => {
            if args.rdf.is_some() {
                RemoteHDTBuilder::new(&args.zarr)
                    .rdf_path(&args.rdf.unwrap())
                    .array_name(&args.array_name)
                    .build()
                    .serialize()?
            } else {
                return Err(String::from("Please provide an RDF path"));
            }
        }
        (_, true) => RemoteHDTBuilder::new(&args.zarr)
            .array_name(&args.array_name)
            .build()
            .parse()?,
        _ => unreachable!(),
    };

    Ok(())
}
