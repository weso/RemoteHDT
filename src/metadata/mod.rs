


use std::default;
use std::path::PathBuf;
use std::str::FromStr;


use crate::dictionary::Dictionary;

use crate::io::Graph;
use crate::io::RdfParser;

use crate::storage::params::Serialization;
use crate::storage::params::Backend;
use crate::storage::params::ReferenceSystem;
use crate::error::RemoteHDTError;

use fcsd::Set;
use zarrs::opendal::raw::oio::StreamExt;
use zarrs::opendal::services::Fs;
use zarrs::opendal::services::Http;
use zarrs::opendal::Operator;
use zarrs::storage::store::OpendalStore;
use zarrs::array::Array;



pub type MetadataResult<T> = Result<T, RemoteHDTError>;

pub struct Metadata {
    flatten_graph: Vec<(u32, u32, u32)>,
    serialization: Serialization,
    dictionary : Dictionary,
    array: Option<Array<OpendalStore>>
}


impl Metadata{
    pub fn new( serialization: Serialization) -> Self {
        Metadata {
           flatten_graph: Vec::<(u32, u32, u32)>::default(),
           serialization: serialization,
           dictionary: Dictionary::default(),
           array: None,
        }
    }
    
    
  
   
    pub fn serialize(&mut self, rdf_path: &str, reference_system: ReferenceSystem, metadata_path: &str, fields: Vec<&str>) -> MetadataResult<&mut Self>{

        let graph_vector: Graph;

        match RdfParser::parse(rdf_path, &reference_system) {
            Ok((graph, dictionary)) => {
                graph_vector = graph;
                self.dictionary = dictionary;
            }
            Err(_) => return Err(RemoteHDTError::RdfParse),
        };

        let mut count = 0;
        for i in graph_vector.iter() {
            for j in i.iter(){
                self.flatten_graph.push((count, j.0, j.1))
            }
            count +=1;
        }


        Ok(self)
    }


   

    

    
}