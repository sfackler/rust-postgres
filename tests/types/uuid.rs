extern crate uuid;

use types::test_type;

#[test]
fn test_uuid_params() {
    test_type("UUID", &[(Some(uuid::Uuid::parse_str("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap()),
                        "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"),
                       (None, "NULL")])
}

#[test]
fn test_uuidarray_params() {
    fn make_check<'a>(uuid: &'a str) -> (uuid::Uuid, &'a str) {
        (uuid::Uuid::parse_str(uuid).unwrap(), uuid)
    }
    let (v1, s1) = make_check("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    let (v2, s2) = make_check("00000000-0000-0000-0000-000000000000");
    let (v3, s3) = make_check("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    test_array_params!("UUID", v1, s1, v2, s2, v3, s3);
}

