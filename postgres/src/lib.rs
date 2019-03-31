//!
#![warn(clippy::all, rust_2018_idioms, missing_docs)]

#[cfg(feature = "runtime")]
use lazy_static::lazy_static;
#[cfg(feature = "runtime")]
use tokio::runtime::{self, Runtime};

pub use tokio_postgres::{error, row, tls, types, Column, Portal, SimpleQueryMessage, Statement};

pub use crate::client::*;
#[cfg(feature = "runtime")]
pub use crate::config::Config;
pub use crate::copy_out_reader::*;
#[doc(no_inline)]
pub use crate::error::Error;
pub use crate::query_iter::*;
pub use crate::query_portal_iter::*;
#[doc(no_inline)]
pub use crate::row::{Row, SimpleQueryRow};
pub use crate::simple_query_iter::*;
#[doc(no_inline)]
pub use crate::tls::NoTls;
pub use crate::to_statement::*;
pub use crate::transaction::*;

mod client;
#[cfg(feature = "runtime")]
pub mod config;
mod copy_out_reader;
mod query_iter;
mod query_portal_iter;
mod simple_query_iter;
mod to_statement;
mod transaction;

#[cfg(feature = "runtime")]
#[cfg(test)]
mod test;

#[cfg(feature = "runtime")]
lazy_static! {
    static ref RUNTIME: Runtime = runtime::Builder::new()
        .name_prefix("postgres-")
        .build()
        .unwrap();
}
