#[derive(Default)]
pub struct MetadataDimensionality {
    pub (crate) metadata_size: usize,
    pub (crate) fields_size : usize
}

impl MetadataDimensionality {

    pub fn new(number_of_triples: usize, number_of_fields: usize)-> Self{
        MetadataDimensionality{
            metadata_size: number_of_triples,
            fields_size: number_of_fields,
        }
    }

    pub fn get_metadata_size(&self)-> u64{
        self.metadata_size as u64
    }


    pub fn get_fields_size(&self) -> u64{
        self.fields_size as u64
    }

}