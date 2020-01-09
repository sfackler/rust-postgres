use crate::{Client, Error, Transaction};

/// The isolation level of a database transaction.
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum IsolationLevel {
    /// Equivalent to `ReadCommitted`.
    ReadUncommitted,

    /// An individual statement in the transaction will see rows committed before it began.
    ReadCommitted,

    /// All statements in the transaction will see the same view of rows committed before the first query in the
    /// transaction.
    RepeatableRead,

    /// The reads and writes in this transaction must be able to be committed as an atomic "unit" with respect to reads
    /// and writes of all other concurrent serializable transactions without interleaving.
    Serializable,
}

/// A builder for database transactions.
pub struct TransactionBuilder<'a> {
    client: &'a mut Client,
    isolation_level: Option<IsolationLevel>,
    read_only: bool,
    deferrable: bool,
}

impl<'a> TransactionBuilder<'a> {
    pub(crate) fn new(client: &'a mut Client) -> TransactionBuilder<'a> {
        TransactionBuilder {
            client,
            isolation_level: None,
            read_only: false,
            deferrable: false,
        }
    }

    /// Sets the isolation level of the transaction.
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.isolation_level = Some(isolation_level);
        self
    }

    /// Sets the transaction to read-only.
    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    /// Sets the transaction to be deferrable.
    ///
    /// If the transaction is also serializable and read only, creation of the transaction may block, but when it
    /// completes the transaction is able to run with less overhead and a guarantee that it will not be aborted due to
    /// serialization failure.
    pub fn deferrable(mut self) -> Self {
        self.deferrable = true;
        self
    }

    /// Begins the transaction.
    ///
    /// The transaction will roll back by default - use the `commit` method to commit it.
    pub async fn start(self) -> Result<Transaction<'a>, Error> {
        let mut query = "START TRANSACTION".to_string();
        let mut first = true;

        if let Some(level) = self.isolation_level {
            first = false;

            query.push_str(" ISOLATION LEVEL ");
            let level = match level {
                IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
                IsolationLevel::ReadCommitted => "READ COMMITTED",
                IsolationLevel::RepeatableRead => "REPEATABLE READ",
                IsolationLevel::Serializable => "SERIALIZABLE",
            };
            query.push_str(level);
        }

        if self.read_only {
            if !first {
                query.push(',');
            }
            first = false;

            query.push_str(" READ ONLY");
        }

        if self.deferrable {
            if !first {
                query.push(',');
            }

            query.push_str(" DEFERRABLE");
        }

        self.client.batch_execute(&query).await?;

        Ok(Transaction::new(self.client))
    }
}
