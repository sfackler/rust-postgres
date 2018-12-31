#[cfg(feature = "runtime")]
use std::time::Duration;
#[cfg(feature = "runtime")]
use tokio_postgres::TargetSessionAttrs;

#[test]
fn pairs_ok() {
    let params = r"user=foo password=' fizz \'buzz\\ ' application_name = ''"
        .parse::<tokio_postgres::Config>()
        .unwrap();

    let mut expected = tokio_postgres::Config::new();
    expected
        .user("foo")
        .password(r" fizz 'buzz\ ")
        .application_name("");

    assert_eq!(params, expected);
}

#[test]
fn pairs_ws() {
    let params = " user\t=\r\n\x0bfoo \t password = hunter2 "
        .parse::<tokio_postgres::Config>()
        .unwrap();

    let mut expected = tokio_postgres::Config::new();
    expected.user("foo").password("hunter2");

    assert_eq!(params, expected);
}

#[test]
#[cfg(feature = "runtime")]
fn settings() {
    let params =
        "connect_timeout=3 keepalives=0 keepalives_idle=30 target_session_attrs=read-write"
            .parse::<tokio_postgres::Config>()
            .unwrap();

    let mut expected = tokio_postgres::Config::new();
    expected
        .connect_timeout(Duration::from_secs(3))
        .keepalives(false)
        .keepalives_idle(Duration::from_secs(30))
        .target_session_attrs(TargetSessionAttrs::ReadWrite);

    assert_eq!(params, expected);
}
