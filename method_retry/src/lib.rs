

// //! Derive macro for the struct-metadata package.

// #![warn(missing_docs, non_ascii_idents, trivial_numeric_casts,
//     noop_method_call, single_use_lifetimes, trivial_casts,
//     unused_lifetimes, nonstandard_style, variant_size_differences)]
// #![deny(keyword_idents)]
// #![warn(clippy::missing_docs_in_private_items)]
// #![allow(clippy::needless_return, clippy::while_let_on_iterator)]

// extern crate proc_macro;
// use proc_macro::TokenStream;
// use syn::{parse_macro_input, DeriveInput};
// use quote::quote;

// #[proc_macro_attribute]
// pub fn retry(attr: TokenStream, item: TokenStream) -> TokenStream {
//     let DeriveInput {ident, attrs, data, ..} = parse_macro_input!(item);

//     let pub_set = ;

//     let output = quote! {
//         #pub_set #async_set fn #name(#self_name) {
//             data,
//         }
//     };

//     output.into()
// }