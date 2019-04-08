use tokio_postgres::Error;

use crate::{Client, Statement};

mod sealed {
    pub trait Sealed {}
}

/// A trait abstracting over prepared and unprepared statements.
///
/// Many methods are generic over this bound, so that they support both a raw query string as well as a statement which
/// was prepared previously.
///
/// This trait is "sealed" and cannot be implemented by anything outside this crate.
pub trait ToStatement: sealed::Sealed {
    #[doc(hidden)]
    fn __statement(&self, client: &Client) -> Result<Statement, Error>;
}

impl sealed::Sealed for str {}

impl ToStatement for str {
    fn __statement(&self, client: &Client) -> Result<Statement, Error> {
        client.prepare(self)
    }
}

impl sealed::Sealed for Statement {}

impl ToStatement for Statement {
    fn __statement(&self, _: &Client) -> Result<Statement, Error> {
        Ok(self.clone())
    }
}
