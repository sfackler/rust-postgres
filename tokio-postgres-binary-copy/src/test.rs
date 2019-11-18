use crate::BinaryCopyStream;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls};

async fn connect() -> Client {
    let (client, connection) =
        tokio_postgres::connect("host=localhost port=5433 user=postgres", NoTls)
            .await
            .unwrap();
    tokio::spawn(async {
        connection.await.unwrap();
    });
    client
}

#[tokio::test]
async fn write_basic() {
    let client = connect().await;

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id INT, bar TEXT)")
        .await
        .unwrap();

    let stream = BinaryCopyStream::new(&[Type::INT4, Type::TEXT], |mut w| {
        async move {
            w.write(&1i32).await?;
            w.write(&"foobar").await?;

            w.write(&2i32).await?;
            w.write(&None::<&str>).await?;

            Ok(())
        }
    });

    client
        .copy_in("COPY foo (id, bar) FROM STDIN BINARY", &[], stream)
        .await
        .unwrap();

    let rows = client.query("SELECT id, bar FROM foo ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, Option<&str>>(1), Some("foobar"));
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, Option<&str>>(1), None);
}

#[tokio::test]
async fn write_many_rows() {
    let client = connect().await;

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id INT, bar TEXT)")
        .await
        .unwrap();

    let stream = BinaryCopyStream::new(&[Type::INT4, Type::TEXT], |mut w| async move {
        for i in 0..10_000i32 {
            w.write(&i).await?;
            w.write(&format!("the value for {}", i)).await?;
        }

        Ok(())
    });

    client.copy_in("COPY foo (id, bar) FROM STDIN BINARY", &[], stream).await.unwrap();

    let rows = client.query("SELECT id, bar FROM foo ORDER BY id", &[]).await.unwrap();
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<_, i32>(0), i as i32);
        assert_eq!(row.get::<_, &str>(1), format!("the value for {}", i));
    }
}

#[tokio::test]
async fn write_big_rows() {
    let client = connect().await;

    client.batch_execute("CREATE TEMPORARY TABLE foo (id INT, bar BYTEA)").await.unwrap();

    let stream = BinaryCopyStream::new(&[Type::INT4, Type::BYTEA], |mut w| {
        async move {
            for i in 0..2i32 {
                w.write(&i).await.unwrap();
                w.write(&vec![i as u8; 128 * 1024]).await.unwrap();
            }

            Ok(())
        }
    });

    client.copy_in("COPY foo (id, bar) FROM STDIN BINARY", &[], stream).await.unwrap();

    let rows = client.query("SELECT id, bar FROM foo ORDER BY id", &[]).await.unwrap();
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<_, i32>(0), i as i32);
        assert_eq!(row.get::<_, &[u8]>(1), &*vec![i as u8; 128 * 1024]);
    }
}
