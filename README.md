This project contains an exploration on ways to replicate [HDT](https://www.rdfhdt.org/) using [ZARR](https://zarr-specs.readthedocs.io/).

1. We have to be able to import data from RDF dumps
2. Then we have to load them into the Application
3. Lastly, the loaded data should be serialized into ZARR

This project could be divided into two main crates:

1. RemoteHDT --> The HDT fork using ZARR
2. rdf-rs --> utilities for importing RDF dumps using Rust

---

    .
    ├── \*.zarr # Resulting Zarr project
    ├── rdf-rs # Crate for importing the RDF dumps into the system
    ├── examples
    ├── src
    │ ├── zarr # All the Zarr utilities
    │ └── main.rs # Main application for creating the Zarr project
    └── ...

---

X axis --> subjects
Y axis --> predicates
Z axis --> objects

!Caveat Unique values should be stored in each of the axis

For each triple, a 1 will be set to each (X, Y, Z) such that (X, Y, Z) = (s, p, o).
-1 otherwise.

---

## Sprint 1

1. ~~Support several systems of reference; namely, SPO, POS, OSP...~~
2. ~~Explore the [Linked Data Fragments](https://linkeddatafragments.org/concept/) project~~
3. Streaming + Filtering = Larger than RAM?
4. ~~Quality attributes: synchronization, size of the original dump...~~
5. ~~[HDTCat](https://arxiv.org/pdf/1809.06859.pdf) --> Larger than RAM HDT, while RemoteHDT --> Remote HDTCat?~~
6. Serverless Linked Data Fragments?
7. ~~[Benchmarking](https://www.w3.org/wiki/RdfStoreBenchmarking)~~
8. Store the HashSet inside the Zarr directory (somewhere)

## Sprint 2

1. Work on the quality attributes and features that we are good at

## Sprint 4

1. LUBM benchmarks
