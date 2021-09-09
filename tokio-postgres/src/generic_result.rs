//! GenericResult

use crate::Row;

#[derive(Debug)]
pub enum GenericResult {
    Row(Row),
    NumRows(u64),
}
