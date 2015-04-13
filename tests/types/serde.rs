extern crate serde;

use self::serde::json::{self, Value};
use types::test_type;

#[test]
fn test_json_params() {
    test_type("JSON", &[(Some(json::from_str::<Value>("[10, 11, 12]").unwrap()),
                        "'[10, 11, 12]'"),
                       (Some(json::from_str::<Value>("{\"f\": \"asd\"}").unwrap()),
                        "'{\"f\": \"asd\"}'"),
                       (None, "NULL")])
}

#[test]
fn test_jsonb_params() {
    test_type("JSONB", &[(Some(json::from_str::<Value>("[10, 11, 12]").unwrap()),
                          "'[10, 11, 12]'"),
                         (Some(json::from_str::<Value>("{\"f\": \"asd\"}").unwrap()),
                          "'{\"f\": \"asd\"}'"),
                         (None, "NULL")])
}
