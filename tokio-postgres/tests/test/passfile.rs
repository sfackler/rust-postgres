use tokio_postgres::NoTls;

async fn check_connects(s: &str) {
    let _ = tokio_postgres::connect(s, NoTls).await.unwrap();
}

async fn check_fails(s: &str) {
    match tokio_postgres::connect(s, NoTls).await {
        Ok(_) => panic!("unexpected success"),
        Err(e) => assert_eq!(e.to_string(), "invalid configuration: password missing"),
    }
}

#[tokio::test]
async fn passfile_with_plain_password() {
    check_connects(
        "host=localhost port=5433 user=pass_user dbname=postgres passfile=../test/pgpass",
    )
    .await;
}

#[tokio::test]
async fn passfile_with_md5_password() {
    check_connects(
        "host=localhost port=5433 user=md5_user dbname=postgres passfile=../test/pgpass",
    )
    .await;
}

#[tokio::test]
async fn passfile_with_scram_password() {
    check_connects(
        "host=localhost port=5433 user=scram_user dbname=postgres passfile=../test/pgpass",
    )
    .await;
}

#[tokio::test]
async fn explicit_password_is_preferred_to_passfile() {
    check_connects("host=localhost port=5433 user=pass_user dbname=postgres passfile=../test/pgpass_wrong password=password").await;
}

#[tokio::test]
async fn passfile_with_no_match() {
    check_fails(
        "host=localhost port=5433 user=md5_user dbname=postgres passfile=../test/pgpass_wrong",
    )
    .await;
}

#[tokio::test]
async fn missing_passfile_is_ignored() {
    check_fails(
        "host=localhost port=5433 user=pass_user dbname=postgres passfile=../test/pgpass_missing",
    )
    .await;
}

#[tokio::test]
async fn directory_as_passfile_is_ignored() {
    check_fails("host=localhost port=5433 user=pass_user dbname=postgres passfile=../test").await;
}
