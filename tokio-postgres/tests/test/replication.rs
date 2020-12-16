use postgres_protocol::message::backend::ReplicationMessage;
use tokio::stream::StreamExt;
use tokio_postgres::Client;
use tokio_postgres::replication_client::ReplicationClient;
use tokio_postgres::{connect, connect_replication, NoTls, ReplicationMode};

const LOGICAL_BEGIN_TAG: u8 = b'B';
const LOGICAL_COMMIT_TAG: u8 = b'C';
const LOGICAL_INSERT_TAG: u8 = b'I';

// Tests missing for timeline_history(). For a timeline history to be
// available, it requires a point-in-time-recovery or a standby
// promotion; neither of which is done in the current test setup.

// test for:
//   - identify_system
//   - show
//   - slot create/drop
//   - physical replication
#[tokio::test]
async fn physical_replication() {
    let (sclient, mut rclient) = setup(ReplicationMode::Physical).await;

    simple_exec(&sclient, "drop table if exists test_physical_replication").await;
    simple_exec(&sclient, "create table test_physical_replication(i int)").await;

    let identify_system = rclient.identify_system().await.unwrap();
    assert_eq!(identify_system.dbname(), None);
    let show_port = rclient.show("port").await.unwrap();
    assert_eq!(show_port, "5433");

    let slot = "test_physical_slot";
    let _ = rclient.drop_replication_slot(slot, false).await.unwrap();
    let slotdesc = rclient
        .create_physical_replication_slot(slot, false, false)
        .await
        .unwrap();
    assert_eq!(slotdesc.slot_name(), slot);
    assert_eq!(slotdesc.snapshot_name(), None);
    assert_eq!(slotdesc.output_plugin(), None);

    let mut physical_stream = rclient
        .start_physical_replication(None, identify_system.xlogpos(), None)
        .await
        .unwrap();

    let _nrows = sclient
        .execute("insert into test_physical_replication values(1)", &[])
        .await
        .unwrap();

    let mut got_xlogdata = false;
    while let Some(replication_message) = physical_stream.next().await {
        if let ReplicationMessage::XLogData(_) = replication_message.unwrap() {
            got_xlogdata = true;
            break;
        }
    }

    assert!(got_xlogdata);

    let response = physical_stream.stop_replication().await.unwrap();
    assert!(response.is_none());

    // repeat simple command after stream is ended
    let show_port = rclient.show("port").await.unwrap();
    assert_eq!(show_port, "5433");

    simple_exec(&sclient, "drop table if exists test_physical_replication").await;
}

// test for:
//   - create/drop slot
//   X standby_status_update
//   - logical replication
#[tokio::test]
async fn logical_replication() {
    let (sclient, mut rclient) = setup(ReplicationMode::Logical).await;

    simple_exec(&sclient, "drop table if exists test_logical_replication").await;
    simple_exec(&sclient, "drop publication if exists test_logical_pub").await;
    simple_exec(&sclient, "create table test_logical_replication(i int)").await;
    simple_exec(
        &sclient,
        "create publication test_logical_pub for table test_logical_replication",
    )
    .await;

    let identify_system = rclient.identify_system().await.unwrap();
    assert_eq!(identify_system.dbname().unwrap(), "postgres");

    let slot = "test_logical_slot";
    let plugin = "pgoutput";
    let _ = rclient.drop_replication_slot(slot, false).await.unwrap();
    let slotdesc = rclient
        .create_logical_replication_slot(slot, false, plugin, None)
        .await
        .unwrap();
    assert_eq!(slotdesc.slot_name(), slot);
    assert!(slotdesc.snapshot_name().is_some());
    assert_eq!(slotdesc.output_plugin(), Some(plugin));

    let xlog_start = identify_system.xlogpos();
    let options = &vec![
        ("proto_version", "1"),
        ("publication_names", "test_logical_pub"),
    ];

    let mut logical_stream = rclient
        .start_logical_replication(slot, xlog_start, options)
        .await
        .unwrap();

    let _nrows = sclient
        .execute("insert into test_logical_replication values(1)", &[])
        .await
        .unwrap();

    let mut got_begin = false;
    let mut got_insert = false;
    let mut got_commit = false;
    while let Some(replication_message) = logical_stream.next().await {
        if let ReplicationMessage::XLogData(msg) = replication_message.unwrap() {
            match msg.data()[0] {
                LOGICAL_BEGIN_TAG => {
                    assert!(!got_begin);
                    assert!(!got_insert);
                    assert!(!got_commit);
                    got_begin = true;
                }
                LOGICAL_INSERT_TAG => {
                    assert!(got_begin);
                    assert!(!got_insert);
                    assert!(!got_commit);
                    got_insert = true;
                }
                LOGICAL_COMMIT_TAG => {
                    assert!(got_begin);
                    assert!(got_insert);
                    assert!(!got_commit);
                    got_commit = true;
                    break;
                }
                _ => (),
            }
        }
    }

    assert!(got_begin);
    assert!(got_insert);
    assert!(got_commit);

    simple_exec(&sclient, "drop table if exists test_logical_replication").await;
    simple_exec(&sclient, "drop publication if exists test_logical_pub").await;
}

// test for base backup
#[tokio::test]
async fn base_backup() {}

// Test that a dropped replication stream properly returns to normal
// command processing in the ReplicationClient.
//
// This test will fail on PostgreSQL server versions earlier than the
// following patch versions: 13.2, 12.6, 11.11, 10.16, 9.6.21,
// 9.5.25. In earlier server versions, there's a bug that prevents
// pipelining requests after the client sends a CopyDone message, but
// before the server replies with a CommandComplete.
//
// Disabled until the patch is more widely available.
// #[tokio::test]
#[allow(dead_code)]
async fn drop_replication_stream() {
    let (sclient, mut rclient) = setup(ReplicationMode::Physical).await;

    simple_exec(&sclient, "drop table if exists test_drop_stream").await;
    simple_exec(&sclient, "create table test_drop_stream(i int)").await;

    let identify_system = rclient.identify_system().await.unwrap();
    assert_eq!(identify_system.dbname(), None);

    let mut physical_stream = rclient
        .start_physical_replication(None, identify_system.xlogpos(), None)
        .await
        .unwrap();

    let mut got_xlogdata = false;
    while let Some(replication_message) = physical_stream.next().await {
        if let ReplicationMessage::XLogData(_) = replication_message.unwrap() {
            got_xlogdata = true;
            break;
        }
    }

    assert!(got_xlogdata);

    drop(physical_stream);

    // test that simple command completes after replication stream is dropped
    let show_port = rclient.show("port").await.unwrap();
    assert_eq!(show_port, "5433");

    simple_exec(&sclient, "drop table if exists test_drop_stream").await;
}

async fn setup(mode: ReplicationMode) -> (Client, ReplicationClient) {
    let conninfo = "host=127.0.0.1 port=5433 user=postgres";

    // form SQL connection
    let (sclient, sconnection) = connect(conninfo, NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = sconnection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // form replication connection
    let (rclient, rconnection) = connect_replication(conninfo, NoTls, mode).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = rconnection.await {
            eprintln!("connection error: {}", e);
        }
    });

    (sclient, rclient)
}

async fn simple_exec(sclient: &Client, command: &str) {
    let _nrows = sclient.execute(command, &[]).await.unwrap();
}
