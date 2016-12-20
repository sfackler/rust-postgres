extern crate serde_json;

use self::serde_json::Value;
use types::test_type;

#[test]
fn test_json_params() {
    test_type("JSON", &[(Some(serde_json::from_str::<Value>("[10, 11, 12]").unwrap()),
                        "'[10, 11, 12]'"),
                       (Some(serde_json::from_str::<Value>("{\"f\": \"asd\"}").unwrap()),
                        "'{\"f\": \"asd\"}'"),
                       (None, "NULL")])
}

#[test]
fn test_jsonb_params() {
    test_type("JSONB", &[(Some(serde_json::from_str::<Value>("[10, 11, 12]").unwrap()),
                          "'[10, 11, 12]'"),
                         (Some(serde_json::from_str::<Value>("{\"f\": \"asd\"}").unwrap()),
                          "'{\"f\": \"asd\"}'"),
                         (None, "NULL")])
}
