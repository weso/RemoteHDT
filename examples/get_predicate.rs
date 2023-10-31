use remote_hdt::{
    engine::EngineStrategy, reference_system::ReferenceSystem, remote_hdt::RemoteHDTBuilder,
};

pub fn main() {
    let ans = RemoteHDTBuilder::new("root.zarr")
        .unwrap()
        .reference_system(ReferenceSystem::SPO)
        .array_name("array_name")
        .build()
        .parse()
        .unwrap()
        .get_predicate(0, ReferenceSystem::SPO);

    println!("{:?}", ans)
}
