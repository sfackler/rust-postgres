#[cfg(feature = "runtime")]
use lazy_static::lazy_static;
#[cfg(feature = "runtime")]
use tokio::runtime::{self, Runtime};

#[cfg(feature = "runtime")]
mod builder;
mod client;
mod statement;

#[cfg(feature = "runtime")]
pub use crate::builder::*;
pub use crate::client::*;
pub use crate::statement::*;

#[cfg(feature = "runtime")]
lazy_static! {
    static ref RUNTIME: Runtime = runtime::Builder::new()
        .name_prefix("postgres-")
        .build()
        .unwrap();
}
