use proc_macro::TokenStream;
use quote::quote;
use syn::{self, DeriveInput};

#[proc_macro_derive(Layout)]
pub fn storage(tokens: TokenStream) -> TokenStream {
    let ast: DeriveInput = syn::parse(tokens).unwrap();
    let name = &ast.ident;

    quote! {
        impl LayoutFields for #name {
            fn set_dictionary(&mut self, dictionary: Dictionary) {
                self.dictionary = dictionary;
            }

            fn get_dictionary(&self) -> Dictionary {
                self.dictionary.to_owned()
            }

            fn set_graph(&mut self, graph: Graph) {
                self.graph = graph;
            }

            fn get_graph(&self) -> Graph {
                self.graph.to_owned()
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
