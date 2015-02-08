use serialize::json::Json;

use types::test_type;

#[test]
fn test_json_params() {
    test_type("JSON", &[(Some(Json::from_str("[10, 11, 12]").unwrap()),
                        "'[10, 11, 12]'"),
                       (Some(Json::from_str("{\"f\": \"asd\"}").unwrap()),
                        "'{\"f\": \"asd\"}'"),
                       (None, "NULL")])
}

#[test]
fn test_jsonb_params() {
    if option_env!("TRAVIS").is_some() { return } // Travis doesn't have Postgres 9.4 yet
    test_type("JSONB", &[(Some(Json::from_str("[10, 11, 12]").unwrap()),
                          "'[10, 11, 12]'"),
                         (Some(Json::from_str("{\"f\": \"asd\"}").unwrap()),
                          "'{\"f\": \"asd\"}'"),
                         (None, "NULL")])
}
