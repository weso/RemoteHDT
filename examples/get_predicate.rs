use remote_hdt::{engine::EngineStrategy, remote_hdt::RemoteHDTBuilder};

pub fn main() {
    let ans = RemoteHDTBuilder::new("root.zarr")
        .unwrap()
        .array_name("array_name")
        .build()
        .parse()
        .unwrap()
        .get_predicate(0);

    println!("{:?}", ans)
}
