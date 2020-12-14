use futures_util::StreamExt;
use std::time::SystemTime;

use postgres_protocol::message::backend::LogicalReplicationMessage::{Begin, Commit, Insert};
use postgres_protocol::message::backend::ReplicationMessage::*;
use postgres_protocol::message::backend::TupleData;
use postgres_types::PgLsn;
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::NoTls;
use tokio_postgres::SimpleQueryMessage::Row;

#[tokio::test]
async fn test_replication() {
    // form SQL connection
    let conninfo = "host=127.0.0.1 port=5433 user=postgres replication=database";
    let (client, connection) = tokio_postgres::connect(conninfo, NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
        .simple_query("DROP TABLE IF EXISTS test_logical_replication")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE test_logical_replication(i int)")
        .await
        .unwrap();
    let res = client
        .simple_query("SELECT 'test_logical_replication'::regclass::oid")
        .await
        .unwrap();
    let rel_id: u32 = if let Row(row) = &res[0] {
        row.get("oid").unwrap().parse().unwrap()
    } else {
        panic!("unexpeced query message");
    };

    client
        .simple_query("DROP PUBLICATION IF EXISTS test_pub")
        .await
        .unwrap();
    client
        .simple_query("CREATE PUBLICATION test_pub FOR ALL TABLES")
        .await
        .unwrap();

    let slot = "test_logical_slot";

    let query = format!(
        r#"CREATE_REPLICATION_SLOT {:?} TEMPORARY LOGICAL "pgoutput""#,
        slot
    );
    let slot_query = client.simple_query(&query).await.unwrap();
    let lsn = if let Row(row) = &slot_query[0] {
        row.get("consistent_point").unwrap()
    } else {
        panic!("unexpeced query message");
    };

    // issue a query that will appear in the slot's stream since it happened after its creation
    client
        .simple_query("INSERT INTO test_logical_replication VALUES (42)")
        .await
        .unwrap();

    let options = r#"("proto_version" '1', "publication_names" 'test_pub')"#;
    let query = format!(
        r#"START_REPLICATION SLOT {:?} LOGICAL {} {}"#,
        slot, lsn, options
    );
    let copy_stream = client
        .copy_both_simple::<bytes::Bytes>(&query)
        .await
        .unwrap();

    let stream = LogicalReplicationStream::new(copy_stream);
    tokio::pin!(stream);

    // verify that we can observe the transaction in the replication stream
    let begin = loop {
        match stream.next().await {
            Some(Ok(XLogData(body))) => {
                if let Begin(begin) = body.into_data() {
                    break begin;
                }
            }
            Some(Ok(_)) => (),
            Some(Err(_)) => panic!("unexpected replication stream error"),
            None => panic!("unexpected replication stream end"),
        }
    };
    let insert = loop {
        match stream.next().await {
            Some(Ok(XLogData(body))) => {
                if let Insert(insert) = body.into_data() {
                    break insert;
                }
            }
            Some(Ok(_)) => (),
            Some(Err(_)) => panic!("unexpected replication stream error"),
            None => panic!("unexpected replication stream end"),
        }
    };

    let commit = loop {
        match stream.next().await {
            Some(Ok(XLogData(body))) => {
                if let Commit(commit) = body.into_data() {
                    break commit;
                }
            }
            Some(Ok(_)) => (),
            Some(Err(_)) => panic!("unexpected replication stream error"),
            None => panic!("unexpected replication stream end"),
        }
    };

    assert_eq!(begin.final_lsn(), commit.commit_lsn());
    assert_eq!(insert.rel_id(), rel_id);

    let tuple_data = insert.tuple().tuple_data();
    assert_eq!(tuple_data.len(), 1);
    assert!(matches!(tuple_data[0], TupleData::Text(_)));
    if let TupleData::Text(data) = &tuple_data[0] {
        assert_eq!(data, &b"42"[..]);
    }

    // Send a standby status update and require a keep alive response
    let lsn: PgLsn = lsn.parse().unwrap();
    stream
        .as_mut()
        .standby_status_update(lsn, lsn, lsn, SystemTime::now(), 1)
        .await
        .unwrap();
    loop {
        match stream.next().await {
            Some(Ok(PrimaryKeepAlive(_))) => break,
            Some(Ok(_)) => (),
            Some(Err(e)) => panic!("unexpected replication stream error: {}", e),
            None => panic!("unexpected replication stream end"),
        }
    }
}
