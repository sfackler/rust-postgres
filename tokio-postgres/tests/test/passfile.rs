#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{fs::OpenOptions, io::Write};

use tokio_postgres::{Error, NoTls};

const PASSFILE: &str = r#"
localhost:5433:*:pass_user:password
localhost:5433:*:md5_user:password
localhost:5433:*:scram_user:password
"#;

const PASSFILE_WRONG: &str = r#"
localhost:5433:*:pass_user:wrong password
"#;

async fn connect_with_passfile(
    passfile_content: &str,
    mode: u32,
    config: &str,
) -> Result<(), Error> {
    let tempdir = tempfile::Builder::new()
        .prefix("tokio-postgres-tests.")
        .tempdir()
        .unwrap();
    let passfile_name = tempdir.path().join("pgpass");
    let mut open_options = OpenOptions::new();
    open_options.write(true).create(true);
    #[cfg(unix)]
    {
        open_options.mode(mode);
    }
    let mut passfile = open_options.open(&passfile_name).unwrap();
    passfile.write_all(passfile_content.as_bytes()).unwrap();
    let config = config.replace("{}", &passfile_name.to_str().unwrap());
    tokio_postgres::connect(&config, NoTls).await.map(|_| ())
}

async fn check_connects(passfile_content: &str, config: &str) {
    connect_with_passfile(passfile_content, 0o0600, config)
        .await
        .unwrap();
}

fn assert_password_missing<T>(result: Result<T, Error>) {
    match result {
        Ok(_) => panic!("unexpected success"),
        Err(e) => assert_eq!(e.to_string(), "invalid configuration: password missing"),
    }
}

#[tokio::test]
async fn passfile_with_plain_password() {
    check_connects(
        PASSFILE,
        "host=localhost port=5433 user=pass_user dbname=postgres passfile={}",
    )
    .await;
}

#[tokio::test]
async fn passfile_with_md5_password() {
    check_connects(
        PASSFILE,
        "host=localhost port=5433 user=md5_user dbname=postgres passfile={}",
    )
    .await;
}

#[tokio::test]
async fn passfile_with_scram_password() {
    check_connects(
        PASSFILE,
        "host=localhost port=5433 user=scram_user dbname=postgres passfile={}",
    )
    .await;
}

#[tokio::test]
async fn explicit_password_is_preferred_to_passfile() {
    check_connects(
        PASSFILE_WRONG,
        "host=localhost port=5433 user=pass_user dbname=postgres passfile={} password=password",
    )
    .await;
}

#[tokio::test]
async fn passfile_with_no_match() {
    assert_password_missing(
        connect_with_passfile(
            PASSFILE_WRONG,
            0o600,
            "host=localhost port=5433 user=md5_user dbname=postgres passfile={}",
        )
        .await,
    )
}

#[tokio::test]
async fn missing_passfile_is_ignored() {
    assert_password_missing(
        tokio_postgres::connect(
            "host=localhost port=5433 user=pass_user dbname=postgres passfile=nosuchfile",
            NoTls,
        )
        .await,
    )
}

#[tokio::test]
async fn directory_as_passfile_is_ignored() {
    assert_password_missing(
        tokio_postgres::connect(
            "host=localhost port=5433 user=pass_user dbname=postgres passfile=.",
            NoTls,
        )
        .await,
    )
}

#[cfg(unix)]
#[tokio::test]
async fn passfile_with_lax_permissions() {
    assert_password_missing(
        connect_with_passfile(
            PASSFILE,
            0o640,
            "host=localhost port=5433 user=pass_user dbname=postgres passfile={}",
        )
        .await,
    )
}
