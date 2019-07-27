use serde_json_1::Value;

use crate::types::test_type;

#[tokio::test]
async fn test_json_params() {
    test_type(
        "JSON",
        vec![
            (
                Some(serde_json_1::from_str::<Value>("[10, 11, 12]").unwrap()),
                "'[10, 11, 12]'",
            ),
            (
                Some(serde_json_1::from_str::<Value>("{\"f\": \"asd\"}").unwrap()),
                "'{\"f\": \"asd\"}'",
            ),
            (None, "NULL"),
        ],
    )
    .await
}

#[tokio::test]
async fn test_jsonb_params() {
    test_type(
        "JSONB",
        vec![
            (
                Some(serde_json_1::from_str::<Value>("[10, 11, 12]").unwrap()),
                "'[10, 11, 12]'",
            ),
            (
                Some(serde_json_1::from_str::<Value>("{\"f\": \"asd\"}").unwrap()),
                "'{\"f\": \"asd\"}'",
            ),
            (None, "NULL"),
        ],
    )
    .await
}
