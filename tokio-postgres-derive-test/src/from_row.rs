use futures_util::FutureExt;

use tokio_postgres::FromRow;
use tokio_postgres::{Client, NoTls};

async fn connect(s: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(s, NoTls).await.unwrap();
    let connection = connection.map(|e| e.unwrap());
    tokio::spawn(connection);

    client
}

async fn query_row<T: FromRow>() -> Result<Vec<T>, tokio_postgres::Error> {
    let client = connect("user=postgres host=localhost port=5433").await;
    client
        .batch_execute(
            "CREATE TEMPORARY TABLE person (
                id serial,
                name text,
                age integer
            );
            INSERT INTO person (name, age) VALUES ('steven', 18);
            ",
        )
        .await
        .unwrap();

    client
        .query_as::<T>("SELECT name, age FROM person", &[])
        .await
}

#[tokio::test]
async fn query_all_as() {
    #[derive(FromRow)]
    struct Person {
        name: String,
        age: i32,
    }

    let users = query_row::<Person>().await.unwrap();

    assert_eq!(users.len(), 1);
    let user = users.first().unwrap();
    assert_eq!(user.name, "steven");
    assert_eq!(user.age, 18);
}

#[tokio::test]
async fn query_all_as_generic() {
    #[derive(FromRow)]
    struct Person<A> {
        name: String,
        age: A,
    }

    let users = query_row::<Person<i32>>().await.unwrap();

    assert_eq!(users.len(), 1);
    let user = users.first().unwrap();
    assert_eq!(user.name, "steven");
    assert_eq!(user.age, 18);
}

#[tokio::test]
async fn query_all_as_from() {
    #[derive(Debug, PartialEq)]
    struct Age(i32);

    impl From<i32> for Age {
        fn from(value: i32) -> Self {
            Self(value)
        }
    }

    #[derive(FromRow)]
    struct Person {
        name: String,
        #[from_row(from = "i32")]
        age: Age,
    }

    let users = query_row::<Person>().await.unwrap();

    assert_eq!(users.len(), 1);
    let user = users.first().unwrap();
    assert_eq!(user.name, "steven");
    assert_eq!(user.age, Age(18));
}

#[tokio::test]
async fn query_all_as_flatten() {
    #[derive(FromRow)]
    struct AgeDetails {
        age: i32,
    }

    #[derive(FromRow)]
    struct Person {
        name: String,
        #[from_row(flatten)]
        age_details: AgeDetails,
    }

    let users = query_row::<Person>().await.unwrap();

    assert_eq!(users.len(), 1);
    let user = users.first().unwrap();
    assert_eq!(user.name, "steven");
    assert_eq!(user.age_details.age, 18);
}

#[tokio::test]
async fn query_all_as_skip() {
    struct NoFromSql {
        message: &'static str,
    }

    impl std::default::Default for NoFromSql {
        fn default() -> Self {
            Self {
                message: "no from sql",
            }
        }
    }

    #[derive(FromRow)]
    struct Person {
        name: String,
        age: i32,
        #[from_row(skip)]
        no_from_sql: NoFromSql,
    }

    let users = query_row::<Person>().await.unwrap();

    assert_eq!(users.len(), 1);
    let user = users.first().unwrap();
    assert_eq!(user.name, "steven");
    assert_eq!(user.age, 18);
    assert_eq!(user.no_from_sql.message, "no from sql");
}
