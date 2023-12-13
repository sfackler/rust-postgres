use futures_util::FutureExt;

use tokio_postgres::FromRow;
use tokio_postgres::{Client, NoTls};

#[tokio::test]
async fn query_all_as() {
    #[derive(FromRow)]
    struct Person {
        name: String,
        age: i32,
    }

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

    let users: Vec<Person> = client
        .query_as("SELECT name, age FROM person", &[])
        .await
        .unwrap();

    assert_eq!(users.len(), 1);
    let user = users.get(0).unwrap();
    assert_eq!(user.name, "steven");
    assert_eq!(user.age, 18)
}

async fn connect(s: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(s, NoTls).await.unwrap();
    let connection = connection.map(|e| e.unwrap());
    tokio::spawn(connection);

    client
}
