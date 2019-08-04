use tokio_postgres::Error;

use crate::{Client, Statement, Transaction};

mod sealed {
    pub trait Sealed {}
}

#[doc(hidden)]
pub trait Prepare {
    fn prepare(&mut self, query: &str) -> Result<Statement, Error>;
}

impl Prepare for Client {
    fn prepare(&mut self, query: &str) -> Result<Statement, Error> {
        self.prepare(query)
    }
}

impl<'a> Prepare for Transaction<'a> {
    fn prepare(&mut self, query: &str) -> Result<Statement, Error> {
        self.prepare(query)
    }
}

/// A trait abstracting over prepared and unprepared statements.
///
/// Many methods are generic over this bound, so that they support both a raw query string as well as a statement which
/// was prepared previously.
///
/// This trait is "sealed" and cannot be implemented by anything outside this crate.
pub trait ToStatement: sealed::Sealed {
    #[doc(hidden)]
    fn __statement<T>(&self, client: &mut T) -> Result<Statement, Error>
    where
        T: Prepare;
}

impl sealed::Sealed for str {}

impl ToStatement for str {
    fn __statement<T>(&self, client: &mut T) -> Result<Statement, Error>
    where
        T: Prepare,
    {
        client.prepare(self)
    }
}

impl sealed::Sealed for Statement {}

impl ToStatement for Statement {
    fn __statement<T>(&self, _: &mut T) -> Result<Statement, Error>
    where
        T: Prepare,
    {
        Ok(self.clone())
    }
}
