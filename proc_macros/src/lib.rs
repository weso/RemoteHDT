use proc_macro2::TokenStream;
use quote::quote;
use syn::{self, DeriveInput};

#[proc_macro_derive(Layout)]
pub fn storage_internals(tokens: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast: DeriveInput = syn::parse(tokens).unwrap();
    storage(ast).into()
}

fn storage(ast: DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let (impl_generics, _, _) = ast.generics.split_for_impl();

    quote! {
        impl #impl_generics LayoutFields for #name {
            fn set_dictionary(&mut self, dictionary: Dictionary) {
                self.dictionary = dictionary;
            }

            fn get_dictionary(&self) -> Dictionary {
                self.dictionary.to_owned()
            }

            fn set_triples_count(&mut self, triples_count: u64) {
                self.triples_count = triples_count;
            }

            fn get_triples_count(&self) -> u64 {
                self.triples_count
            }

            fn set_rdf_path(&mut self, rdf_path: String) {
                self.rdf_path = rdf_path.to_string();
            }

            fn get_rdf_path(&self) -> String {
                self.rdf_path.to_owned()
            }
        }
    }
    .into()
}
