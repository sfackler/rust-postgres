use std::collections::HashMap;

#[test]
fn pairs_ok() {
    let params = r"user=foo password=' fizz \'buzz\\ ' thing = ''"
        .parse::<tokio_postgres::Builder>()
        .unwrap();
    let params = params.iter().collect::<HashMap<_, _>>();

    let mut expected = HashMap::new();
    expected.insert("user", "foo");
    expected.insert("password", r" fizz 'buzz\ ");
    expected.insert("thing", "");
    expected.insert("client_encoding", "UTF8");
    expected.insert("timezone", "GMT");

    assert_eq!(params, expected);
}

#[test]
fn pairs_ws() {
    let params = " user\t=\r\n\x0bfoo \t password = hunter2 "
        .parse::<tokio_postgres::Builder>()
        .unwrap();;
    let params = params.iter().collect::<HashMap<_, _>>();

    let mut expected = HashMap::new();
    expected.insert("user", "foo");
    expected.insert("password", r"hunter2");
    expected.insert("client_encoding", "UTF8");
    expected.insert("timezone", "GMT");

    assert_eq!(params, expected);
}
