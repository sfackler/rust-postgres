extern crate linked_hash_map;
extern crate marksman_escape;
extern crate phf_codegen;
extern crate regex;

use std::path::Path;

mod sqlstate;
mod type_gen;

fn main() {
    let path = Path::new("../tokio-postgres/src");
    sqlstate::build(path);
    type_gen::build(path);
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
