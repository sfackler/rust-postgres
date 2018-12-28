#[test]
fn pairs_ok() {
    let params = r"user=foo password=' fizz \'buzz\\ ' thing = ''"
        .parse::<tokio_postgres::Builder>()
        .unwrap();

    let mut expected = tokio_postgres::Builder::new();
    expected
        .param("user", "foo")
        .password(r" fizz 'buzz\ ")
        .param("thing", "");

    assert_eq!(params, expected);
}

#[test]
fn pairs_ws() {
    let params = " user\t=\r\n\x0bfoo \t password = hunter2 "
        .parse::<tokio_postgres::Builder>()
        .unwrap();;

    let mut expected = tokio_postgres::Builder::new();
    expected.param("user", "foo").password("hunter2");

    assert_eq!(params, expected);
}
