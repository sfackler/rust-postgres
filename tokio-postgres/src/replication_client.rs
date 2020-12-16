//! Streaming replication support.
//!
//! This module allows writing Postgres replication clients. A
//! replication client forms a special connection to the server in
//! either physical replication mode, which receives a stream of raw
//! Write-Ahead Log (WAL) records; or logical replication mode, which
//! receives a stream of data that depends on the output plugin
//! selected. All data and control messages are exchanged in CopyData
//! envelopes.
//!
//! See the [PostgreSQL protocol
//! documentation](https://www.postgresql.org/docs/current/protocol-replication.html)
//! for details of the protocol itself.
//!
//! # Physical Replication Client Example
//! ```no_run
//! extern crate tokio;
//!
//! use postgres_protocol::message::backend::ReplicationMessage;
//! use tokio::stream::StreamExt;
//! use tokio_postgres::{connect_replication, Error, NoTls, ReplicationMode};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let conninfo = "host=localhost user=postgres dbname=postgres";
//!
//!     // form replication connection
//!     let (mut rclient, rconnection) =
//!         connect_replication(conninfo, NoTls, ReplicationMode::Physical).await?;
//!     tokio::spawn(async move {
//!         if let Err(e) = rconnection.await {
//!             eprintln!("connection error: {}", e);
//!         }
//!     });
//!
//!     let identify_system = rclient.identify_system().await?;
//!
//!     let mut physical_stream = rclient
//!         .start_physical_replication(None, identify_system.xlogpos(), None)
//!         .await?;
//!
//!     while let Some(replication_message) = physical_stream.next().await {
//!         match replication_message? {
//!             ReplicationMessage::XLogData(xlog_data) => {
//!                 eprintln!("received XLogData: {:#?}", xlog_data);
//!             }
//!             ReplicationMessage::PrimaryKeepAlive(keepalive) => {
//!                 eprintln!("received PrimaryKeepAlive: {:#?}", keepalive);
//!             }
//!             _ => (),
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Logical Replication Client Example
//!
//! This example requires the [wal2json
//! extension](https://github.com/eulerto/wal2json).
//!
//! ```no_run
//! extern crate tokio;
//!
//! use postgres_protocol::message::backend::ReplicationMessage;
//! use tokio::stream::StreamExt;
//! use tokio_postgres::{connect_replication, Error, NoTls, ReplicationMode};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let conninfo = "host=localhost user=postgres dbname=postgres";
//!
//!     // form replication connection
//!     let (mut rclient, rconnection) =
//!         connect_replication(conninfo, NoTls, ReplicationMode::Logical).await?;
//!
//!     // spawn connection to run on its own
//!     tokio::spawn(async move {
//!         if let Err(e) = rconnection.await {
//!             eprintln!("connection error: {}", e);
//!         }
//!     });
//!
//!     let identify_system = rclient.identify_system().await?;
//!
//!     let slot = "my_slot";
//!     let plugin = "wal2json";
//!     let options = &vec![("pretty-print", "1")];
//!
//!     let _slotdesc = rclient
//!         .create_logical_replication_slot(slot, false, plugin, None)
//!         .await?;
//!
//!     let mut physical_stream = rclient
//!         .start_logical_replication(slot, identify_system.xlogpos(), options)
//!         .await?;
//!
//!     while let Some(replication_message) = physical_stream.next().await {
//!         match replication_message? {
//!             ReplicationMessage::XLogData(xlog_data) => {
//!                 eprintln!("received XLogData: {:#?}", xlog_data);
//!                 let json = std::str::from_utf8(xlog_data.data()).unwrap();
//!                 eprintln!("JSON text: {}", json);
//!             }
//!             ReplicationMessage::PrimaryKeepAlive(keepalive) => {
//!                 eprintln!("received PrimaryKeepAlive: {:#?}", keepalive);
//!             }
//!             _ => (),
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Caveats
//!
//! It is recommended that you use a PostgreSQL server patch version
//! of at least: 14.0, 13.2, 12.6, 11.11, 10.16, 9.6.21, or 9.5.25.

use crate::client::Responses;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::{Lsn, Type};
use crate::{simple_query, Client, Error};
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use futures::{ready, Stream};
use pin_project::{pin_project, pinned_drop};
use postgres_protocol::escape::{escape_identifier, escape_literal};
use postgres_protocol::message::backend::{Message, ReplicationMessage};
use postgres_protocol::message::frontend;
use std::marker::PhantomPinned;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::from_utf8;
use std::task::{Context, Poll};

/// Result of [identify_system()](ReplicationClient::identify_system()) call.
#[derive(Debug)]
pub struct IdentifySystem {
    systemid: String,
    timeline: u32,
    xlogpos: Lsn,
    dbname: Option<String>,
}

impl IdentifySystem {
    pub fn systemid(&self) -> &str {
        &self.systemid
    }

    pub fn timeline(&self) -> u32 {
        self.timeline
    }

    pub fn xlogpos(&self) -> Lsn {
        self.xlogpos
    }

    pub fn dbname(&self) -> Option<&str> {
        self.dbname.as_deref()
    }
}

/// Result of [timeline_history()](ReplicationClient::timeline_history()) call.
#[derive(Debug)]
pub struct TimelineHistory {
    filename: PathBuf,
    content: Vec<u8>,
}

impl TimelineHistory {
    pub fn filename(&self) -> &Path {
        self.filename.as_path()
    }

    pub fn content(&self) -> &[u8] {
        self.content.as_slice()
    }
}

/// Argument to
/// [create_logical_replication_slot()](ReplicationClient::create_logical_replication_slot).
#[derive(Debug)]
pub enum SnapshotMode {
    ExportSnapshot,
    NoExportSnapshot,
    UseSnapshot,
}

/// Description of slot created with
/// [create_physical_replication_slot()](ReplicationClient::create_physical_replication_slot)
/// or
/// [create_logical_replication_slot()](ReplicationClient::create_logical_replication_slot).
#[derive(Debug)]
pub struct CreateReplicationSlotResponse {
    slot_name: String,
    consistent_point: Lsn,
    snapshot_name: Option<String>,
    output_plugin: Option<String>,
}

impl CreateReplicationSlotResponse {
    pub fn slot_name(&self) -> &str {
        &self.slot_name
    }

    pub fn consistent_point(&self) -> Lsn {
        self.consistent_point
    }

    pub fn snapshot_name(&self) -> Option<&str> {
        self.snapshot_name.as_deref()
    }

    pub fn output_plugin(&self) -> Option<&str> {
        self.output_plugin.as_deref()
    }
}

/// Represents a client connected in replication mode.
pub struct ReplicationClient {
    client: Client,
    replication_stream_active: bool,
}

impl ReplicationClient {
    pub(crate) fn new(client: Client) -> ReplicationClient {
        ReplicationClient {
            client: client,
            replication_stream_active: false,
        }
    }
}

impl ReplicationClient {
    /// IDENTIFY_SYSTEM message
    pub async fn identify_system(&mut self) -> Result<IdentifySystem, Error> {
        let command = "IDENTIFY_SYSTEM";
        let mut responses = self.send(command).await?;
        let rowdesc = match responses.next().await? {
            Message::RowDescription(m) => m,
            m => return Err(Error::unexpected_message(m)),
        };
        let datarow = match responses.next().await? {
            Message::DataRow(m) => m,
            m => return Err(Error::unexpected_message(m)),
        };
        match responses.next().await? {
            Message::CommandComplete(_) => (),
            m => return Err(Error::unexpected_message(m)),
        };
        match responses.next().await? {
            Message::ReadyForQuery(_) => (),
            m => return Err(Error::unexpected_message(m)),
        };

        let fields = rowdesc.fields().collect::<Vec<_>>().map_err(Error::parse)?;
        let ranges = datarow.ranges().collect::<Vec<_>>().map_err(Error::parse)?;

        assert_eq!(fields.len(), 4);
        assert_eq!(fields[0].type_oid(), Type::TEXT.oid());
        assert_eq!(fields[0].format(), 0);
        assert_eq!(fields[1].type_oid(), Type::INT4.oid());
        assert_eq!(fields[1].format(), 0);
        assert_eq!(fields[2].type_oid(), Type::TEXT.oid());
        assert_eq!(fields[2].format(), 0);
        assert_eq!(fields[3].type_oid(), Type::TEXT.oid());
        assert_eq!(fields[3].format(), 0);
        assert_eq!(ranges.len(), 4);

        let values: Vec<Option<&str>> = ranges
            .iter()
            .map(|range| {
                range
                    .to_owned()
                    .map(|r| from_utf8(&datarow.buffer()[r]).unwrap())
            })
            .collect::<Vec<_>>();

        Ok(IdentifySystem {
            systemid: values[0].unwrap().to_string(),
            timeline: values[1].unwrap().parse::<u32>().unwrap(),
            xlogpos: Lsn::from(values[2].unwrap()),
            dbname: values[3].map(String::from),
        })
    }

    /// show the value of the given setting
    pub async fn show(&mut self, name: &str) -> Result<String, Error> {
        let command = format!("SHOW {}", escape_identifier(name));
        let mut responses = self.send(&command).await?;
        let rowdesc = match responses.next().await? {
            Message::RowDescription(m) => m,
            m => return Err(Error::unexpected_message(m)),
        };
        let datarow = match responses.next().await? {
            Message::DataRow(m) => m,
            m => return Err(Error::unexpected_message(m)),
        };
        match responses.next().await? {
            Message::CommandComplete(_) => (),
            m => return Err(Error::unexpected_message(m)),
        };
        match responses.next().await? {
            Message::ReadyForQuery(_) => (),
            m => return Err(Error::unexpected_message(m)),
        };

        let fields = rowdesc.fields().collect::<Vec<_>>().map_err(Error::parse)?;
        let ranges = datarow.ranges().collect::<Vec<_>>().map_err(Error::parse)?;

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].type_oid(), Type::TEXT.oid());
        assert_eq!(ranges.len(), 1);

        let val = from_utf8(&datarow.buffer()[ranges[0].to_owned().unwrap()]).unwrap();

        Ok(String::from(val))
    }

    /// show the value of the given setting
    pub async fn timeline_history(&mut self, timeline_id: u32) -> Result<TimelineHistory, Error> {
        let command = format!("TIMELINE_HISTORY {}", timeline_id);
        let mut responses = self.send(&command).await?;

        let rowdesc = match responses.next().await? {
            Message::RowDescription(m) => m,
            m => return Err(Error::unexpected_message(m)),
        };
        let datarow = match responses.next().await? {
            Message::DataRow(m) => m,
            m => return Err(Error::unexpected_message(m)),
        };
        match responses.next().await? {
            Message::CommandComplete(_) => (),
            m => return Err(Error::unexpected_message(m)),
        };
        match responses.next().await? {
            Message::ReadyForQuery(_) => (),
            m => return Err(Error::unexpected_message(m)),
        };

        let fields = rowdesc.fields().collect::<Vec<_>>().map_err(Error::parse)?;
        let ranges = datarow.ranges().collect::<Vec<_>>().map_err(Error::parse)?;

        assert_eq!(fields.len(), 2);

        assert_eq!(fields[0].type_oid(), Type::TEXT.oid());
        assert_eq!(fields[0].format(), 0);
        assert_eq!(fields[1].type_oid(), Type::TEXT.oid());
        assert_eq!(fields[1].format(), 0);

        assert_eq!(ranges.len(), 2);

        let filename = &datarow.buffer()[ranges[0].to_owned().unwrap()];
        let content = &datarow.buffer()[ranges[1].to_owned().unwrap()];

        let filename_path = PathBuf::from(from_utf8(filename).unwrap());

        Ok(TimelineHistory {
            filename: filename_path,
            content: Vec::from(content),
        })
    }

    /// Create physical replication slot
    pub async fn create_physical_replication_slot(
        &mut self,
        slot_name: &str,
        temporary: bool,
        reserve_wal: bool,
    ) -> Result<CreateReplicationSlotResponse, Error> {
        let temporary_str = if temporary { " TEMPORARY" } else { "" };
        let reserve_wal_str = if reserve_wal { " RESERVE_WAL" } else { "" };
        let command = format!(
            "CREATE_REPLICATION_SLOT {}{} PHYSICAL{}",
            escape_identifier(slot_name),
            temporary_str,
            reserve_wal_str
        );
        let mut responses = self.send(&command).await?;

        let rowdesc = match responses.next().await? {
            Message::RowDescription(m) => m,
            m => return Err(Error::unexpected_message(m)),
        };
        let datarow = match responses.next().await? {
            Message::DataRow(m) => m,
            m => return Err(Error::unexpected_message(m)),
        };
        match responses.next().await? {
            Message::CommandComplete(_) => (),
            m => return Err(Error::unexpected_message(m)),
        };
        match responses.next().await? {
            Message::ReadyForQuery(_) => (),
            m => return Err(Error::unexpected_message(m)),
        };

        let fields = rowdesc.fields().collect::<Vec<_>>().map_err(Error::parse)?;
        let ranges = datarow.ranges().collect::<Vec<_>>().map_err(Error::parse)?;

        assert_eq!(fields.len(), 4);

        let values: Vec<Option<&str>> = ranges
            .iter()
            .map(|range| {
                range
                    .to_owned()
                    .map(|r| from_utf8(&datarow.buffer()[r]).unwrap())
            })
            .collect::<Vec<_>>();

        Ok(CreateReplicationSlotResponse {
            slot_name: values[0].unwrap().to_string(),
            consistent_point: Lsn::from(values[1].unwrap()),
            snapshot_name: values[2].map(String::from),
            output_plugin: values[3].map(String::from),
        })
    }

    /// Create logical replication slot.
    pub async fn create_logical_replication_slot(
        &mut self,
        slot_name: &str,
        temporary: bool,
        plugin_name: &str,
        snapshot_mode: Option<SnapshotMode>,
    ) -> Result<CreateReplicationSlotResponse, Error> {
        let temporary_str = if temporary { " TEMPORARY" } else { "" };
        let snapshot_str = snapshot_mode.map_or("", |mode| match mode {
            SnapshotMode::ExportSnapshot => " EXPORT_SNAPSHOT",
            SnapshotMode::NoExportSnapshot => " NOEXPORT_SNAPSHOT",
            SnapshotMode::UseSnapshot => " USE_SNAPSHOT",
        });
        let command = format!(
            "CREATE_REPLICATION_SLOT {}{} LOGICAL {}{}",
            escape_identifier(slot_name),
            temporary_str,
            escape_identifier(plugin_name),
            snapshot_str
        );
        let mut responses = self.send(&command).await?;

        let rowdesc = match responses.next().await? {
            Message::RowDescription(m) => m,
            m => return Err(Error::unexpected_message(m)),
        };
        let datarow = match responses.next().await? {
            Message::DataRow(m) => m,
            m => return Err(Error::unexpected_message(m)),
        };
        match responses.next().await? {
            Message::CommandComplete(_) => (),
            m => return Err(Error::unexpected_message(m)),
        };
        match responses.next().await? {
            Message::ReadyForQuery(_) => (),
            m => return Err(Error::unexpected_message(m)),
        };

        let fields = rowdesc.fields().collect::<Vec<_>>().map_err(Error::parse)?;
        let ranges = datarow.ranges().collect::<Vec<_>>().map_err(Error::parse)?;

        assert_eq!(fields.len(), 4);

        let values: Vec<Option<&str>> = ranges
            .iter()
            .map(|range| {
                range
                    .to_owned()
                    .map(|r| from_utf8(&datarow.buffer()[r]).unwrap())
            })
            .collect::<Vec<_>>();

        Ok(CreateReplicationSlotResponse {
            slot_name: values[0].unwrap().to_string(),
            consistent_point: Lsn::from(values[1].unwrap()),
            snapshot_name: values[2].map(String::from),
            output_plugin: values[3].map(String::from),
        })
    }

    /// Drop replication slot
    pub async fn drop_replication_slot(
        &mut self,
        slot_name: &str,
        wait: bool,
    ) -> Result<(), Error> {
        let wait_str = if wait { " WAIT" } else { "" };
        let command = format!(
            "DROP_REPLICATION_SLOT {}{}",
            escape_identifier(slot_name),
            wait_str
        );
        let _ = self.send(&command).await?;
        Ok(())
    }

    /// Begin physical replication, consuming the replication client and producing a replication stream.
    ///
    /// Replication begins starting with the given Log Sequence Number
    /// (LSN) on the given timeline.
    pub async fn start_physical_replication<'a>(
        &'a mut self,
        slot_name: Option<&str>,
        lsn: Lsn,
        timeline_id: Option<u32>,
    ) -> Result<Pin<Box<ReplicationStream<'a>>>, Error> {
        let slot = match slot_name {
            Some(name) => format!(" SLOT {}", escape_identifier(name)),
            None => String::from(""),
        };
        let timeline = match timeline_id {
            Some(id) => format!(" TIMELINE {}", id),
            None => String::from(""),
        };
        let command = format!(
            "START_REPLICATION{} PHYSICAL {}{}",
            slot,
            String::from(lsn),
            timeline
        );

        Ok(self.start_replication(command).await?)
    }

    /// Begin logical replication, consuming the replication client and producing a replication stream.
    ///
    /// Replication begins starting with the given Log Sequence Number
    /// (LSN) on the current timeline.
    pub async fn start_logical_replication<'a>(
        &'a mut self,
        slot_name: &str,
        lsn: Lsn,
        options: &[(&str, &str)],
    ) -> Result<Pin<Box<ReplicationStream<'a>>>, Error> {
        let slot = format!(" SLOT {}", escape_identifier(slot_name));
        let options_string = if !options.is_empty() {
            format!(
                " ({})",
                options
                    .iter()
                    .map(|pair| format!("{} {}", escape_identifier(pair.0), escape_literal(pair.1)))
                    .collect::<Vec<String>>()
                    .as_slice()
                    .join(", ")
            )
        } else {
            String::from("")
        };
        let command = format!(
            "START_REPLICATION{} LOGICAL {}{}",
            slot,
            String::from(lsn),
            options_string
        );

        Ok(self.start_replication(command).await?)
    }

    /// Send update to server.
    pub async fn standby_status_update(
        &mut self,
        write_lsn: Lsn,
        flush_lsn: Lsn,
        apply_lsn: Lsn,
        ts: i64,
        reply: u8,
    ) -> Result<(), Error> {
        let iclient = self.client.inner();
        let mut buf = BytesMut::new();
        let _ = frontend::standby_status_update(
            write_lsn.into(),
            flush_lsn.into(),
            apply_lsn.into(),
            ts as i64,
            reply,
            &mut buf,
        );
        let _ = iclient.send(RequestMessages::Single(FrontendMessage::Raw(buf.freeze())))?;
        Ok(())
    }

    // Private methods

    // send command to the server, but finish any unfinished replication stream, first
    async fn send(&mut self, command: &str) -> Result<Responses, Error> {
        let iclient = self.client.inner();
        let buf = simple_query::encode(iclient, command)?;
        let responses = iclient.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;
        Ok(responses)
    }

    async fn start_replication<'a>(
        &'a mut self,
        command: String,
    ) -> Result<Pin<Box<ReplicationStream<'a>>>, Error> {
        let mut responses = self.send(&command).await?;
        self.replication_stream_active = true;

        match responses.next().await? {
            Message::CopyBothResponse(_) => {}
            m => return Err(Error::unexpected_message(m)),
        }

        Ok(Box::pin(ReplicationStream {
            rclient: self,
            responses: responses,
            _phantom_pinned: PhantomPinned,
        }))
    }

    fn send_copydone(&mut self) -> Result<(), Error> {
        if self.replication_stream_active {
            let iclient = self.client.inner();
            let mut buf = BytesMut::new();
            frontend::copy_done(&mut buf);
            iclient
                .unpipelined_send(RequestMessages::Single(FrontendMessage::Raw(buf.freeze())))?;
            self.replication_stream_active = false;
        }
        Ok(())
    }
}

/// A stream of data from a `START_REPLICATION` command. All control
/// and data messages will be in
/// [CopyData](postgres_protocol::message::backend::Message::CopyData).
///
/// Intended to be used with the [next()](tokio::stream::StreamExt::next) method.
#[pin_project(PinnedDrop)]
pub struct ReplicationStream<'a> {
    rclient: &'a mut ReplicationClient,
    responses: Responses,
    #[pin]
    _phantom_pinned: PhantomPinned,
}

impl ReplicationStream<'_> {
    /// Stop replication stream and return the replication client object.
    pub async fn stop_replication(mut self: Pin<Box<Self>>) -> Result<(), Error> {
        let this = self.as_mut().project();

        this.rclient.send_copydone()?;
        let responses = this.responses;

        // drain remaining CopyData messages and CopyDone
        loop {
            match responses.next().await? {
                Message::CopyData(_) => (),
                Message::CopyDone => break,
                m => return Err(Error::unexpected_message(m)),
            }
        }

        match responses.next().await? {
            Message::CommandComplete(_) => (),
            m => return Err(Error::unexpected_message(m)),
        };
        match responses.next().await? {
            Message::CommandComplete(_) => (),
            m => return Err(Error::unexpected_message(m)),
        };
        match responses.next().await? {
            Message::ReadyForQuery(_) => (),
            m => return Err(Error::unexpected_message(m)),
        };

        Ok(())
    }
}

impl Stream for ReplicationStream<'_> {
    type Item = Result<ReplicationMessage, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let responses = this.responses;

        match ready!(responses.poll_next(cx)?) {
            Message::CopyData(body) => {
                let r = ReplicationMessage::parse(&body.into_bytes());
                Poll::Ready(Some(r.map_err(Error::parse)))
            }
            Message::CopyDone => Poll::Ready(None),
            m => Poll::Ready(Some(Err(Error::unexpected_message(m)))),
        }
    }
}

#[pinned_drop]
impl PinnedDrop for ReplicationStream<'_> {
    fn drop(mut self: Pin<&mut Self>) {
        let this = self.project();
        this.rclient.send_copydone().unwrap();
    }
}
