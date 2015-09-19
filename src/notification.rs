//! Asynchronous notifications.

use std::fmt;

use {desynchronized, Result, Connection, NotificationsNew};
use message::BackendMessage::NotificationResponse;
use error::Error;

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
        fmt.debug_struct("Notifications")
            .field("pending", &self.len())
            .finish()
    }
}

impl<'conn> Notifications<'conn> {
    /// Returns the number of pending notifications.
    pub fn len(&self) -> usize {
        self.conn.conn.borrow().notifications.len()
    }

    /// Returns an iterator over pending notifications.
    ///
    /// # Note
    ///
    /// This iterator may start returning `Some` after previously returning
    /// `None` if more notifications are received.
    pub fn iter<'a>(&'a self) -> Iter<'a> {
        Iter {
            conn: self.conn,
        }
    }

    /// Returns an iterator over notifications, blocking until one is received
    /// if none are pending.
    ///
    /// The iterator will never return `None`.
    pub fn blocking_iter<'a>(&'a self) -> BlockingIter<'a> {
        BlockingIter {
            conn: self.conn,
        }
    }
}

impl<'a, 'conn> IntoIterator for &'a Notifications<'conn> {
    type Item = Notification;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

impl<'conn> NotificationsNew<'conn> for Notifications<'conn> {
    fn new(conn: &'conn Connection) -> Notifications<'conn> {
        Notifications {
            conn: conn,
        }
    }
}

/// An iterator over pending notifications.
pub struct Iter<'a> {
    conn: &'a Connection,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Notification;

    fn next(&mut self) -> Option<Notification> {
        self.conn.conn.borrow_mut().notifications.pop_front()
    }
}

/// An iterator over notifications which will block if none are pending.
pub struct BlockingIter<'a> {
    conn: &'a Connection,
}

impl<'a> Iterator for BlockingIter<'a> {
    type Item = Result<Notification>;

    fn next(&mut self) -> Option<Result<Notification>> {
        let mut conn = self.conn.conn.borrow_mut();
        if conn.is_desynchronized() {
            return Some(Err(Error::IoError(desynchronized())));
        }

        if let Some(notification) = conn.notifications.pop_front() {
            return Some(Ok(notification));
        }

        match conn.read_message_with_notification() {
            Ok(NotificationResponse { pid, channel, payload }) => {
                Some(Ok(Notification {
                    pid: pid,
                    channel: channel,
                    payload: payload
                }))
            }
            Err(err) => Some(Err(Error::IoError(err))),
            _ => unreachable!()
        }
    }
}
