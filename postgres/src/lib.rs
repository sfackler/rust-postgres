#[cfg(feature = "runtime")]
use lazy_static::lazy_static;
#[cfg(feature = "runtime")]
use tokio::runtime::{self, Runtime};

mod client;
#[cfg(feature = "runtime")]
mod config;
mod copy_out_reader;
mod portal;
mod query_iter;
mod query_portal_iter;
mod simple_query_iter;
mod statement;
mod to_statement;
mod transaction;

#[cfg(feature = "runtime")]
#[cfg(test)]
mod test;

pub use crate::client::*;
#[cfg(feature = "runtime")]
pub use crate::config::*;
pub use crate::copy_out_reader::*;
pub use crate::portal::*;
pub use crate::query_iter::*;
pub use crate::query_portal_iter::*;
pub use crate::simple_query_iter::*;
pub use crate::statement::*;
pub use crate::to_statement::*;
pub use crate::transaction::*;

#[cfg(feature = "runtime")]
lazy_static! {
    static ref RUNTIME: Runtime = runtime::Builder::new()
        .name_prefix("postgres-")
        .build()
        .unwrap();
}
