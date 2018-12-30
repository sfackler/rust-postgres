#[cfg(feature = "runtime")]
use std::time::Duration;

#[test]
fn pairs_ok() {
    let params = r"user=foo password=' fizz \'buzz\\ ' thing = ''"
        .parse::<tokio_postgres::Config>()
        .unwrap();

    let mut expected = tokio_postgres::Config::new();
    expected
        .param("user", "foo")
        .password(r" fizz 'buzz\ ")
        .param("thing", "");

    assert_eq!(params, expected);
}

#[test]
fn pairs_ws() {
    let params = " user\t=\r\n\x0bfoo \t password = hunter2 "
        .parse::<tokio_postgres::Config>()
        .unwrap();

    let mut expected = tokio_postgres::Config::new();
    expected.param("user", "foo").password("hunter2");

    assert_eq!(params, expected);
}

#[test]
#[cfg(feature = "runtime")]
fn settings() {
    let params = "connect_timeout=3 keepalives=0 keepalives_idle=30"
        .parse::<tokio_postgres::Config>()
        .unwrap();

    let mut expected = tokio_postgres::Config::new();
    expected
        .connect_timeout(Duration::from_secs(3))
        .keepalives(false)
        .keepalives_idle(Duration::from_secs(30));

    assert_eq!(params, expected);
}
