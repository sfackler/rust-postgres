//! Asynchronous notifications.

use debug_builders::DebugStruct;
use std::fmt;

use {Result, Connection, NotificationsNew};
use message::BackendMessage::NotificationResponse;

/// An asynchronous notification.
#[derive(Clone, Debug)]
pub struct Notification {
    /// The process ID of the notifying backend process.
    pub pid: u32,
    /// The name of the channel that the notify has been raised on.
    pub channel: String,
    /// The "payload" string passed from the notifying process.
    pub payload: String,
}

/// An iterator over asynchronous notifications.
pub struct Notifications<'conn> {
    conn: &'conn Connection
}

impl<'a> fmt::Debug for Notifications<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        DebugStruct::new(fmt, "Notifications")
            .field("pending", &self.conn.conn.borrow().notifications.len())
            .finish()
    }
}

impl<'conn> NotificationsNew<'conn> for Notifications<'conn> {
    fn new(conn: &'conn Connection) -> Notifications<'conn> {
        Notifications {
            conn: conn,
        }
    }
}

impl<'conn> Iterator for Notifications<'conn> {
    type Item = Notification;

    /// Returns the oldest pending notification or `None` if there are none.
    ///
    /// ## Note
    ///
    /// `next` may return `Some` notification after returning `None` if a new
    /// notification was received.
    fn next(&mut self) -> Option<Notification> {
        self.conn.conn.borrow_mut().notifications.pop_front()
    }
}

impl<'conn> Notifications<'conn> {
    /// Returns the oldest pending notification.
    ///
    /// If no notifications are pending, blocks until one arrives.
    pub fn next_block(&mut self) -> Result<Notification> {
        if let Some(notification) = self.next() {
            return Ok(notification);
        }

        let mut conn = self.conn.conn.borrow_mut();
        check_desync!(conn);
        match try!(conn.read_message_with_notification()) {
            NotificationResponse { pid, channel, payload } => {
                Ok(Notification {
                    pid: pid,
                    channel: channel,
                    payload: payload
                })
            }
            _ => unreachable!()
        }
    }
}

