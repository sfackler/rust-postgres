use std::path::PathBuf;

use super::*;

const PASSFILE: &str = r#"#
# empty lines are ignored

# the following lines are ignored as ill-formed
exhost.test
exhost.test:5432
exhost.test:5432:exdb
exhost.test:5432:exdb:exuser
# the following lines can't match in practice
exhost.test:nnnn:exdb:exuser:nonnumeric port
exhost.test::exdb:exuser:empty port
# the following line is a comment
#exhost.test:5433:exdb:exuser:not this one
# the following lines are well-formed
exhost.test:5432:exdb:exuser:password-5432
exhost.test:5433:exdb:exuser:interesting € password
exhost.test:5432:exuser:exuser:password-exuserdb
# The following line does not count as a wildcard
exhost.test:5432:\*:exuser:password-escaped-star-dbname
exhost.test:5432:*:exuser:password-star-dbname
exhost.test:5432:*:exusersl:password-backslash\
/run/postgresql:5432:*:exuser:password-unix
# the following line can match (so processing stops), but provides no password
exhost.test:5432:exdb:exuser2:
exhost.test:5432:exdb:exuser2:password-shadowed
exhost.test:5432:exdb:ex\:user:password-colon-user
exhost.test:5432:exdb:ex\\us\er:password-backslash-user
exhost.test:5432:exdb:*:password-star-user
exhost.test:*:exdb:exuser:password-star-port
*:5432:exdb:exuser:password-star-host
exhost.test:5433:exdb:exuser3:password-extra:extra:ignored
exhost.test:5433:exdb:exuser4:p\ass\\word\: with escapes
"#;

const PASSFILE_CRLF: &str = "\
    #exhost.test:5433:exdb:exuser:not this one\r\n\
    \r\n\
    exhost.test:5432:exdb:exuser:crlfpassword1\r\n\
    exhost.test:5433:exdb:exuser:crlfpassword2\r\n\
    exhost.test:5432:exuser:exuser:crlfpassword3\r\n\
    ";

const PASSFILE_NONL: &str = "exhost.test:5432:exdb:exuser:nonewline";

const PASSFILE_NONUTF8: &[u8] = b"exhost.test:5432:exdb:exuser:\xa3100\n";

const PASSFILE_NULS: &str = "
exhost.test:5432:exdb:ex\x00user:null-in-username
exhost.test:5432:exdb:exuser:null-in-password-\x00
exhost.test:5432:exdb:exuser:no-null
";

async fn lookup(
    passfile_content: &str,
    host: &Host,
    port: u16,
    dbname: Option<&str>,
    user: &str,
) -> Option<Vec<u8>>
where
{
    let key = PassfileKey::new(host, port, dbname, user);
    password_for_key(&key, passfile_content.as_bytes()).await
}

async fn check_found(
    passfile_content: &str,
    host: &Host,
    port: u16,
    dbname: Option<&str>,
    user: &str,
) -> String
where
{
    let password = lookup(passfile_content, host, port, dbname, user)
        .await
        .unwrap();
    String::from_utf8(password).unwrap()
}

#[tokio::test]
async fn full_key() {
    let password = check_found(
        PASSFILE,
        &Host::Tcp("exhost.test".to_owned()),
        5433,
        Some("exdb"),
        "exuser",
    )
    .await;
    assert_eq!(password, "interesting € password");
}

#[tokio::test]
async fn no_match() {
    let password = lookup(
        PASSFILE,
        &Host::Tcp("nohost.test".to_owned()),
        5433,
        Some("exdb"),
        "exuser",
    )
    .await;
    assert_eq!(password, None);
}

#[tokio::test]
async fn default_dbname() {
    let password = check_found(
        PASSFILE,
        &Host::Tcp("exhost.test".to_owned()),
        5432,
        None,
        "exuser",
    )
    .await;
    assert_eq!(password, "password-exuserdb");
}

#[tokio::test]
async fn unix_socket_host() {
    let password = check_found(
        PASSFILE,
        &Host::Unix(PathBuf::from("/run/postgresql")),
        5432,
        Some("exdb"),
        "exuser",
    )
    .await;
    assert_eq!(password, "password-unix");
}

#[tokio::test]
async fn star_host() {
    let password = check_found(
        PASSFILE,
        &Host::Tcp("nomatch.test".to_owned()),
        5432,
        Some("exdb"),
        "exuser",
    )
    .await;
    assert_eq!(password, "password-star-host");
}

#[tokio::test]
async fn star_port() {
    let password = check_found(
        PASSFILE,
        &Host::Tcp("exhost.test".to_owned()),
        9999,
        Some("exdb"),
        "exuser",
    )
    .await;
    assert_eq!(password, "password-star-port");
}

#[tokio::test]
async fn star_user() {
    let password = check_found(
        PASSFILE,
        &Host::Tcp("exhost.test".to_owned()),
        5432,
        Some("exdb"),
        "nomatch",
    )
    .await;
    assert_eq!(password, "password-star-user");
}

#[tokio::test]
async fn star_dbname() {
    let password = check_found(
        PASSFILE,
        &Host::Tcp("exhost.test".to_owned()),
        5432,
        Some("nodb"),
        "exuser",
    )
    .await;
    assert_eq!(password, "password-star-dbname");
}

#[tokio::test]
async fn colon_in_user() {
    let password = check_found(
        PASSFILE,
        &Host::Tcp("exhost.test".to_owned()),
        5432,
        Some("exdb"),
        "ex:user",
    )
    .await;
    assert_eq!(password, "password-colon-user");
}

#[tokio::test]
async fn backslash_in_user() {
    let password = check_found(
        PASSFILE,
        &Host::Tcp("exhost.test".to_owned()),
        5432,
        Some("exdb"),
        r#"ex\user"#,
    )
    .await;
    assert_eq!(password, "password-backslash-user");
}

#[tokio::test]
async fn extra_ignored_fields() {
    let password = check_found(
        PASSFILE,
        &Host::Tcp("exhost.test".to_owned()),
        5433,
        Some("exdb"),
        "exuser3",
    )
    .await;
    assert_eq!(password, "password-extra");
}

#[tokio::test]
async fn escapes_in_password() {
    let password = check_found(
        PASSFILE,
        &Host::Tcp("exhost.test".to_owned()),
        5433,
        Some("exdb"),
        "exuser4",
    )
    .await;
    assert_eq!(password, r#"pass\word: with escapes"#);
}

#[tokio::test]
async fn final_backslash_in_password() {
    let password = check_found(
        PASSFILE,
        &Host::Tcp("exhost.test".to_owned()),
        5432,
        Some("exdb"),
        "exusersl",
    )
    .await;
    assert_eq!(password, r#"password-backslash\"#);
}

#[tokio::test]
async fn comment_is_ignored() {
    let password = lookup(
        PASSFILE,
        &Host::Tcp("#exhost.test".to_owned()),
        5433,
        Some("exdb"),
        "exuser",
    )
    .await;
    assert_eq!(password, None);
}

#[tokio::test]
async fn missing_password_field() {
    // If a line matches but the password field is missing, no password is
    // returned and we don't search further.
    let password = lookup(
        PASSFILE,
        &Host::Tcp("exhost.test".to_owned()),
        5432,
        Some("exdb"),
        "exuser2",
    )
    .await;
    assert_eq!(password, None);
}

#[tokio::test]
async fn crlf_line_endings() {
    let password = check_found(
        PASSFILE_CRLF,
        &Host::Tcp("exhost.test".to_owned()),
        5433,
        Some("exdb"),
        "exuser",
    )
    .await;
    assert_eq!(password, "crlfpassword2");
}

#[tokio::test]
async fn no_final_newline() {
    let password = check_found(
        PASSFILE_NONL,
        &Host::Tcp("exhost.test".to_owned()),
        5432,
        Some("exdb"),
        "exuser",
    )
    .await;
    assert_eq!(password, "nonewline");
}

#[tokio::test]
async fn non_utf8_password() {
    let password = password_for_key(
        &PassfileKey::new(
            &Host::Tcp("exhost.test".to_owned()),
            5432,
            Some("exdb"),
            "exuser",
        ),
        PASSFILE_NONUTF8,
    )
    .await
    .unwrap();
    assert_eq!(password, b"\xa3100");
}

#[tokio::test]
async fn nul_characters() {
    let password = check_found(
        PASSFILE_NULS,
        &Host::Tcp("exhost.test".to_owned()),
        5432,
        Some("exdb"),
        "exuser",
    )
    .await;
    assert_eq!(password, "no-null");
}
