The idea is to be able to replicate HDT using ZARR.

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
