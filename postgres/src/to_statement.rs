use tokio_postgres::Error;

use crate::{Client, Statement};

mod sealed {
    pub trait Sealed {}
}

pub trait ToStatement: sealed::Sealed {
    #[doc(hidden)]
    fn __statement(&self, client: &mut Client) -> Result<Statement, Error>;
}

impl sealed::Sealed for str {}

impl ToStatement for str {
    fn __statement(&self, client: &mut Client) -> Result<Statement, Error> {
        client.prepare(self)
    }
}

impl sealed::Sealed for Statement {}

impl ToStatement for Statement {
    fn __statement(&self, _: &mut Client) -> Result<Statement, Error> {
        Ok(self.clone())
    }
}
