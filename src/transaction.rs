//! Transactions

use std::cell::Cell;
use std::fmt;
use std::ascii::AsciiExt;

use {bad_response, Result, Connection, TransactionInternals, ConfigInternals,
     IsolationLevelNew};
use error::Error;
use rows::Rows;
use stmt::Statement;
use types::ToSql;

/// An enumeration of transaction isolation levels.
///
/// See the [Postgres documentation](http://www.postgresql.org/docs/9.4/static/transaction-iso.html)
/// for full details on the semantics of each level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// The "read uncommitted" level.
    ///
    /// In current versions of Postgres, this behaves identically to
    /// `ReadCommitted`.
    ReadUncommitted,
    /// The "read committed" level.
    ///
    /// This is the default isolation level in Postgres.
    ReadCommitted,
    /// The "repeatable read" level.
    RepeatableRead,
    /// The "serializable" level.
    Serializable,
}

impl IsolationLevelNew for IsolationLevel {
    fn new(raw: &str) -> Result<IsolationLevel> {
        if raw.eq_ignore_ascii_case("READ UNCOMMITTED") {
            Ok(IsolationLevel::ReadUncommitted)
        } else if raw.eq_ignore_ascii_case("READ COMMITTED") {
            Ok(IsolationLevel::ReadCommitted)
        } else if raw.eq_ignore_ascii_case("REPEATABLE READ") {
            Ok(IsolationLevel::RepeatableRead)
        } else if raw.eq_ignore_ascii_case("SERIALIZABLE") {
            Ok(IsolationLevel::Serializable)
        } else {
            Err(Error::Io(bad_response()))
        }
    }
}

impl IsolationLevel {
    fn to_sql(&self) -> &'static str {
        match *self {
            IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }
}

/// Configuration of a transaction.
#[derive(Debug)]
pub struct Config {
    isolation_level: Option<IsolationLevel>,
    read_only: Option<bool>,
    deferrable: Option<bool>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            isolation_level: None,
            read_only: None,
            deferrable: None,
        }
    }
}

impl ConfigInternals for Config {
    fn build_command(&self, s: &mut String) {
        let mut first = true;

        if let Some(isolation_level) = self.isolation_level {
            s.push_str(" ISOLATION LEVEL ");
            s.push_str(isolation_level.to_sql());
            first = false;
        }

        if let Some(read_only) = self.read_only {
            if first {
                s.push(',');
            }
            if read_only {
                s.push_str(" READ ONLY");
            } else {
                s.push_str(" READ WRITE");
            }
            first = false;
        }

        if let Some(deferrable) = self.deferrable {
            if first {
                s.push(',');
            }
            if deferrable {
                s.push_str(" DEFERRABLE");
            } else {
                s.push_str(" NOT DEFERRABLE");
            }
        }
    }
}

impl Config {
    /// Creates a new `Config` with no configuration overrides.
    pub fn new() -> Config {
        Config::default()
    }

    /// Sets the isolation level of the configuration.
    pub fn isolation_level(&mut self, isolation_level: IsolationLevel) -> &mut Config {
        self.isolation_level = Some(isolation_level);
        self
    }

    /// Sets the read-only property of a transaction.
    ///
    /// If enabled, a transaction will be unable to modify any persistent
    /// database state.
    pub fn read_only(&mut self, read_only: bool) -> &mut Config {
        self.read_only = Some(read_only);
        self
    }

    /// Sets the deferrable property of a transaction.
    ///
    /// If enabled in a read only, serializable transaction, the transaction may
    /// block when created, after which it will run without the normal overhead
    /// of a serializable transaction and will not be forced to roll back due
    /// to serialization failures.
    pub fn deferrable(&mut self, deferrable: bool) -> &mut Config {
        self.deferrable = Some(deferrable);
        self
    }
}

/// A transaction on a database connection.
///
/// The transaction will roll back by default.
pub struct Transaction<'conn> {
    conn: &'conn Connection,
    depth: u32,
    commit: Cell<bool>,
    finished: bool,
}

impl<'a> fmt::Debug for Transaction<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Transaction")
           .field("commit", &self.commit.get())
           .field("depth", &self.depth)
           .finish()
    }
}

impl<'conn> Drop for Transaction<'conn> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.finish_inner();
        }
    }
}

impl<'conn> TransactionInternals<'conn> for Transaction<'conn> {
    fn new(conn: &'conn Connection, depth: u32) -> Transaction<'conn> {
        Transaction {
            conn: conn,
            depth: depth,
            commit: Cell::new(false),
            finished: false,
        }
    }

    fn conn(&self) -> &'conn Connection {
        self.conn
    }

    fn depth(&self) -> u32 {
        self.depth
    }
}

impl<'conn> Transaction<'conn> {
    fn finish_inner(&mut self) -> Result<()> {
        let mut conn = self.conn.conn.borrow_mut();
        debug_assert!(self.depth == conn.trans_depth);
        let query = match (self.commit.get(), self.depth != 1) {
            (false, true) => "ROLLBACK TO sp",
            (false, false) => "ROLLBACK",
            (true, true) => "RELEASE sp",
            (true, false) => "COMMIT",
        };
        conn.trans_depth -= 1;
        conn.quick_query(query).map(|_| ())
    }

    /// Like `Connection::prepare`.
    pub fn prepare(&self, query: &str) -> Result<Statement<'conn>> {
        self.conn.prepare(query)
    }

    /// Like `Connection::prepare_cached`.
    ///
    /// Note that the statement will be cached for the duration of the
    /// connection, not just the duration of this transaction.
    pub fn prepare_cached(&self, query: &str) -> Result<Statement<'conn>> {
        self.conn.prepare_cached(query)
    }

    /// Like `Connection::execute`.
    pub fn execute(&self, query: &str, params: &[&ToSql]) -> Result<u64> {
        self.conn.execute(query, params)
    }

    /// Like `Connection::query`.
    pub fn query<'a>(&'a self, query: &str, params: &[&ToSql]) -> Result<Rows<'a>> {
        self.conn.query(query, params)
    }

    /// Like `Connection::batch_execute`.
    pub fn batch_execute(&self, query: &str) -> Result<()> {
        self.conn.batch_execute(query)
    }

    /// Like `Connection::transaction`.
    ///
    /// # Panics
    ///
    /// Panics if there is an active nested transaction.
    pub fn transaction<'a>(&'a self) -> Result<Transaction<'a>> {
        let mut conn = self.conn.conn.borrow_mut();
        check_desync!(conn);
        assert!(conn.trans_depth == self.depth,
                "`transaction` may only be called on the active transaction");
        try!(conn.quick_query("SAVEPOINT sp"));
        conn.trans_depth += 1;
        Ok(Transaction {
            conn: self.conn,
            commit: Cell::new(false),
            depth: self.depth + 1,
            finished: false,
        })
    }

    /// Returns a reference to the `Transaction`'s `Connection`.
    pub fn connection(&self) -> &'conn Connection {
        self.conn
    }

    /// Like `Connection::is_active`.
    pub fn is_active(&self) -> bool {
        self.conn.conn.borrow().trans_depth == self.depth
    }

    /// Alters the configuration of the active transaction.
    pub fn set_config(&self, config: &Config) -> Result<()> {
        let mut command = "SET TRANSACTION".to_owned();
        config.build_command(&mut command);
        self.batch_execute(&command)
    }

    /// Determines if the transaction is currently set to commit or roll back.
    pub fn will_commit(&self) -> bool {
        self.commit.get()
    }

    /// Sets the transaction to commit at its completion.
    pub fn set_commit(&self) {
        self.commit.set(true);
    }

    /// Sets the transaction to roll back at its completion.
    pub fn set_rollback(&self) {
        self.commit.set(false);
    }

    /// A convenience method which consumes and commits a transaction.
    pub fn commit(self) -> Result<()> {
        self.set_commit();
        self.finish()
    }

    /// Consumes the transaction, commiting or rolling it back as appropriate.
    ///
    /// Functionally equivalent to the `Drop` implementation of `Transaction`
    /// except that it returns any error to the caller.
    pub fn finish(mut self) -> Result<()> {
        self.finished = true;
        self.finish_inner()
    }
}
