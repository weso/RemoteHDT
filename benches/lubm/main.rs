use criterion::{criterion_group, criterion_main, Criterion};
use remote_hdt::remote_hdt::RemoteHDTBuilder;

const BENCHMARKS: [&str; 1] = ["lubm1000"];

fn lubm_benchmark(c: &mut Criterion) {
    c.bench_function(format!("{}", BENCHMARKS[0]).as_str(), |b| {
        b.iter(|| {
            // Code to benchmark
            RemoteHDTBuilder::new(format!("{}.zarr", BENCHMARKS[0]).as_str())
                .rdf_path(format!("benches/lubm/{}.nt", BENCHMARKS[0]).as_str())
                .array_name("array_name")
                .build()
                .from_rdf()
                .unwrap()
        })
    });
}

criterion_group!(benches, lubm_benchmark);
criterion_main!(benches);
