//! Asynchronous notifications.

use fallible_iterator::{FallibleIterator, IntoFallibleIterator};
use std::fmt;
use std::time::Duration;
use postgres_protocol::message::backend::{self, ErrorFields};
use error::DbError;

#[doc(inline)]
use postgres_shared;
pub use postgres_shared::Notification;

use {desynchronized, Result, Connection};
use error::Error;

/// Notifications from the Postgres backend.
pub struct Notifications<'conn> {
    conn: &'conn Connection,
}

impl<'a> fmt::Debug for Notifications<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Notifications")
            .field("pending", &self.len())
            .finish()
    }
}

impl<'conn> Notifications<'conn> {
    pub(crate) fn new(conn: &'conn Connection) -> Notifications<'conn> {
        Notifications { conn: conn }
    }

    /// Returns the number of pending notifications.
    pub fn len(&self) -> usize {
        self.conn.0.borrow().notifications.len()
    }

    /// Determines if there are any pending notifications.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a fallible iterator over pending notifications.
    ///
    /// # Note
    ///
    /// This iterator may start returning `Some` after previously returning
    /// `None` if more notifications are received.
    pub fn iter<'a>(&'a self) -> Iter<'a> {
        Iter { conn: self.conn }
    }

    /// Returns a fallible iterator over notifications that blocks until one is
    /// received if none are pending.
    ///
    /// The iterator will never return `None`.
    pub fn blocking_iter<'a>(&'a self) -> BlockingIter<'a> {
        BlockingIter { conn: self.conn }
    }

    /// Returns a fallible iterator over notifications that blocks for a limited
    /// time waiting to receive one if none are pending.
    ///
    /// # Note
    ///
    /// This iterator may start returning `Some` after previously returning
    /// `None` if more notifications are received.
    pub fn timeout_iter<'a>(&'a self, timeout: Duration) -> TimeoutIter<'a> {
        TimeoutIter {
            conn: self.conn,
            timeout: timeout,
        }
    }
}

impl<'a, 'conn> IntoFallibleIterator for &'a Notifications<'conn> {
    type Item = Notification;
    type Error = Error;
    type IntoIter = Iter<'a>;

    fn into_fallible_iterator(self) -> Iter<'a> {
        self.iter()
    }
}

/// A fallible iterator over pending notifications.
pub struct Iter<'a> {
    conn: &'a Connection,
}

impl<'a> FallibleIterator for Iter<'a> {
    type Item = Notification;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Notification>> {
        let mut conn = self.conn.0.borrow_mut();

        if let Some(notification) = conn.notifications.pop_front() {
            return Ok(Some(notification));
        }

        if conn.is_desynchronized() {
            return Err(desynchronized().into());
        }

        match conn.read_message_with_notification_nonblocking() {
            Ok(Some(backend::Message::NotificationResponse(body))) => {
                Ok(Some(Notification {
                    process_id: body.process_id(),
                    channel: body.channel()?.to_owned(),
                    payload: body.message()?.to_owned(),
                }))
            }
            Ok(Some(backend::Message::ErrorResponse(body))) => Err(err(&mut body.fields())),
            Ok(None) => Ok(None),
            Err(err) => Err(err.into()),
            _ => unreachable!(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.conn.0.borrow().notifications.len(), None)
    }
}

/// An iterator over notifications which will block if none are pending.
pub struct BlockingIter<'a> {
    conn: &'a Connection,
}

impl<'a> FallibleIterator for BlockingIter<'a> {
    type Item = Notification;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Notification>> {
        let mut conn = self.conn.0.borrow_mut();

        if let Some(notification) = conn.notifications.pop_front() {
            return Ok(Some(notification));
        }

        if conn.is_desynchronized() {
            return Err(desynchronized().into());
        }

        match conn.read_message_with_notification() {
            Ok(backend::Message::NotificationResponse(body)) => {
                Ok(Some(Notification {
                    process_id: body.process_id(),
                    channel: body.channel()?.to_owned(),
                    payload: body.message()?.to_owned(),
                }))
            }
            Ok(backend::Message::ErrorResponse(body)) => Err(err(&mut body.fields())),
            Err(err) => Err(err.into()),
            _ => unreachable!(),
        }
    }
}

/// An iterator over notifications which will block for a period of time if
/// none are pending.
pub struct TimeoutIter<'a> {
    conn: &'a Connection,
    timeout: Duration,
}

impl<'a> FallibleIterator for TimeoutIter<'a> {
    type Item = Notification;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Notification>> {
        let mut conn = self.conn.0.borrow_mut();

        if let Some(notification) = conn.notifications.pop_front() {
            return Ok(Some(notification));
        }

        if conn.is_desynchronized() {
            return Err(desynchronized().into());
        }

        match conn.read_message_with_notification_timeout(self.timeout) {
            Ok(Some(backend::Message::NotificationResponse(body))) => {
                Ok(Some(Notification {
                    process_id: body.process_id(),
                    channel: body.channel()?.to_owned(),
                    payload: body.message()?.to_owned(),
                }))
            }
            Ok(Some(backend::Message::ErrorResponse(body))) => Err(err(&mut body.fields())),
            Ok(None) => Ok(None),
            Err(err) => Err(err.into()),
            _ => unreachable!(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.conn.0.borrow().notifications.len(), None)
    }
}

fn err(fields: &mut ErrorFields) -> Error {
    match DbError::new(fields) {
        Ok(err) => postgres_shared::error::db(err),
        Err(err) => err.into(),
    }
}
