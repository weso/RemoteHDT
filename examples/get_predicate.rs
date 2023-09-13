use remote_hdt::remote_hdt::{Engine, RemoteHDTBuilder};

pub fn main() {
    let ans = RemoteHDTBuilder::new("root.zarr")
        .reference_system(remote_hdt::remote_hdt::ReferenceSystem::SPO)
        .array_name("array_name")
        .build()
        .parse()
        .unwrap()
        .get_predicate(0);

    println!("{:?}", ans)
}
