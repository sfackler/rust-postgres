extern crate futures;
extern crate tokio;
extern crate tokio_postgres;

use futures::{Future, Stream};
use std::str::FromStr;
use tokio_postgres::params::ConnectParams;
use tokio_postgres::{connect, TlsMode};

/// You first need to run `createdb example_select1`
const DB_URL: &str = "postgres://postgres@localhost:5432/example_select1";

fn main() {
    println!("For this example to work, you need to create a database 'example_select1' on localhost first, e.g. using `createdb example_select1`");
    println!("Connecting to postgres url {}", DB_URL);
    let params = ConnectParams::from_str(DB_URL).expect("valid postgres url");
    let future = connect(params, TlsMode::None).and_then(|(mut client, conn)| {
        println!("Got client and connection");
        conn.join({
            println!("Preparing statement");
            client.prepare("SELECT 1 AS one").and_then(move |stmt| {
                println!("Got statement, running query");
                client
                    .query(&stmt, &[])
                    .map(|row| {
                        println!("Got one row");
                        row
                    }).collect()
            })
        }).map(|(_, rows)| rows)
    });
    let rows = future.wait().expect("future to be ok");
    assert_eq!(1, rows.len());
    let row = &rows[0];
    let columns = row.columns();
    for i in 0..row.len() {
        let col = &columns[i];
        println!(
            "Column index: {}, Name: {}, Type: {}, Value: {}",
            i,
            col.name(),
            col.type_(),
            row.get::<_, i32>(i)
        );
    }
}
