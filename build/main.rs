extern crate phf_codegen;
extern crate regex;

use std::ascii::AsciiExt;

mod sqlstate;
mod types;

fn main() {
    sqlstate::build();
    types::build();

    println!("cargo:rerun-if-changed=build");
}

fn snake_to_camel(s: &str) -> String {
    let mut out = String::new();

    let mut upper = true;
    for ch in s.chars() {
        if ch == '_' {
            upper = true;
        } else {
            let ch = if upper {
                upper = false;
                ch.to_ascii_uppercase()
            } else {
                ch
            };
            out.push(ch);
        }
    }

    out
}
