extern crate phf_codegen;

use std::ascii::AsciiExt;
use std::env;
use std::fs::File;
use std::io::{Write, BufWriter};
use std::path::Path;
use std::convert::AsRef;

const ERRCODES_TXT: &'static str = include_str!("errcodes.txt");

struct Code {
    code: String,
    variant: String,
}

fn main() {
    let path = env::var_os("OUT_DIR").unwrap();
    let path: &Path = path.as_ref();
    let path = path.join("sqlstate.rs");
    let mut file = BufWriter::new(File::create(&path).unwrap());

    let codes = parse_codes();

    make_enum(&codes, &mut file);
    make_map(&codes, &mut file);
    make_impl(&codes, &mut file);

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=errcodes.txt");
}

fn parse_codes() -> Vec<Code> {
    let mut codes = vec![];

    for line in ERRCODES_TXT.lines() {
        if line.starts_with("#") || line.starts_with("Section") || line.trim().is_empty() {
            continue;
        }

        let mut it = line.split_whitespace();
        let code = it.next().unwrap().to_owned();
        it.next();
        it.next();
        // for 2202E
        let name = match it.next() {
            Some(name) => name,
            None => continue,
        };
        let variant = match variant_name(&code) {
            Some(variant) => variant,
            None => snake_to_camel(&name),
        };

        codes.push(Code {
            code: code,
            variant: variant,
        });
    }

    codes
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

fn variant_name(code: &str) -> Option<String> {
    match code {
        "01004" => Some("WarningStringDataRightTruncation".to_owned()),
        "22001" => Some("DataStringDataRightTruncation".to_owned()),
        "2F002" => Some("SqlRoutineModifyingSqlDataNotPermitted".to_owned()),
        "38002" => Some("ForeignRoutineModifyingSqlDataNotPermitted".to_owned()),
        "2F003" => Some("SqlRoutineProhibitedSqlStatementAttempted".to_owned()),
        "38003" => Some("ForeignRoutineProhibitedSqlStatementAttempted".to_owned()),
        "2F004" => Some("SqlRoutineReadingSqlDataNotPermitted".to_owned()),
        "38004" => Some("ForeignRoutineReadingSqlDataNotPermitted".to_owned()),
        "22004" => Some("DataNullValueNotAllowed".to_owned()),
        "39004" => Some("ExternalRoutineInvocationNullValueNotAllowed".to_owned()),
        _ => None,
    }
}

fn make_enum(codes: &[Code], file: &mut BufWriter<File>) {
    write!(file,
r#"/// SQLSTATE error codes
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum SqlState {{
"#
           ).unwrap();

    for code in codes {
        write!(file,
"    /// `{}`
    {},\n",
               code.code, code.variant).unwrap();
    }

    write!(file,
"    /// An unknown code
    Other(String)
}}
"
           ).unwrap();
}

fn make_map(codes: &[Code], file: &mut BufWriter<File>) {
    write!(file, "static SQLSTATE_MAP: phf::Map<&'static str, SqlState> = ").unwrap();
    let mut builder = phf_codegen::Map::new();
    for code in codes {
        builder.entry(&*code.code, &format!("SqlState::{}", code.variant));
    }
    builder.build(file).unwrap();
    write!(file, ";\n").unwrap();
}

fn make_impl(codes: &[Code], file: &mut BufWriter<File>) {
    write!(file, r#"
impl SqlState {{
    /// Creates a `SqlState` from its error code.
    pub fn from_code(s: String) -> SqlState {{
        match SQLSTATE_MAP.get(&*s) {{
            Some(state) => state.clone(),
            None => SqlState::Other(s)
        }}
    }}

    /// Returns the error code corresponding to the `SqlState`.
    pub fn code(&self) -> &str {{
        match *self {{"#
           ).unwrap();

    for code in codes {
        write!(file, r#"
            SqlState::{} => "{}","#,
               code.variant, code.code).unwrap();
    }

    write!(file, r#"
            SqlState::Other(ref s) => s,
        }}
    }}
}}
"#
           ).unwrap();
}
