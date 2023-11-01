use remote_hdt::{engine::EngineStrategy, remote_hdt::RemoteHDTBuilder};

pub fn main() {
    let ans = RemoteHDTBuilder::new("root.zarr")
        .unwrap()
        .build()
        .parse()
        .unwrap()
        .get_predicate(0);

    println!("{:?}", ans)
}
