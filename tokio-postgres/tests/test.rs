extern crate env_logger;
extern crate tokio;
extern crate tokio_postgres;

use tokio::prelude::*;
use tokio::runtime::current_thread::Runtime;
use tokio_postgres::types::Type;

#[test]
fn pipelined_prepare() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect("postgres://postgres@localhost:5433".parse().unwrap());
    let (mut client, connection) = runtime.block_on(handshake).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    let prepare1 = client.prepare("SELECT 1::BIGINT WHERE $1::BOOL");
    let prepare2 = client.prepare("SELECT ''::TEXT, 1::FLOAT4 WHERE $1::VARCHAR IS NOT NULL");
    let prepare = prepare1.join(prepare2);
    let (statement1, statement2) = runtime.block_on(prepare).unwrap();

    assert_eq!(statement1.params(), &[Type::BOOL]);
    assert_eq!(statement1.columns().len(), 1);
    assert_eq!(statement1.columns()[0].type_(), &Type::INT8);

    assert_eq!(statement2.params(), &[Type::VARCHAR]);
    assert_eq!(statement2.columns().len(), 2);
    assert_eq!(statement2.columns()[0].type_(), &Type::TEXT);
    assert_eq!(statement2.columns()[1].type_(), &Type::FLOAT4);

    drop(statement1);
    drop(statement2);
    drop(client);
    runtime.run().unwrap();
}
