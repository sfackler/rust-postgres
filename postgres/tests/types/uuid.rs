extern crate uuid;

use types::test_type;

#[test]
fn test_uuid_params() {
    test_type(
        "UUID",
        &[
            (
                Some(
                    uuid::Uuid::parse_str("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap(),
                ),
                "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
            ),
            (None, "NULL"),
        ],
    )
}
