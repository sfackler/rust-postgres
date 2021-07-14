//! Support for reading password files.
//!
//! Requires the `runtime` Cargo feature.
//!
//! See [the PostgreSQL libpq documentation][libpq_pgpass] for details of the file format.
//!
//! This module assumes the passfile is in an ASCII-compatible encoding, but does not require the file as a whole to be
//! UTF-8. Lines with a `dbname`, `user`, or TCP `host` field which is not UTF-8 will never match a search. The
//! retrieved password is returned as a byte string.
//!
//! If a line contains a NUL character it will be ignored (libpq's behavior is erratic for files containing NULs).
//!
//! libpq converts a `host` set to the default socket path (`DEFAULT_PGSOCKET_DIR`) into `"localhost"` for matching
//! purposes. This module doesn't attempt to do that, because we don't know what `DEFAULT_PGSOCKET_DIR` is. So passfiles
//! using `localhost` to supply passwords for unix socket connections won't work.
//!
//! libpq can fall back to matching the `host` field against `hostaddr` or `"localhost"` if `host` is unset. We have no
//! equivalent to this behavior: we require an explicit `host` in the connection configuration.
//!
//! Otherwise, the code here is intended to match libpq's behavior in all corner cases, including malformed files.
//!
//! [libpq_pgpass]: https://www.postgresql.org/docs/current/libpq-pgpass.html

#[cfg(unix)]
use std::os::unix::{ffi::OsStrExt, fs::MetadataExt};
use std::path::Path;

use log::warn;
use tokio::fs::{self, File};
use tokio::io::BufReader;

use crate::config::{Config, Host};

#[cfg(test)]
mod test;

/// The data needed to search for a matching passfile entry.
struct PassfileKey<'a> {
    hostname: &'a [u8],
    port: Vec<u8>,
    dbname: &'a [u8],
    user: &'a [u8],
}

impl<'a> PassfileKey<'a> {
    fn new(host: &'a Host, port: u16, dbname: Option<&'a str>, user: &'a str) -> PassfileKey<'a> {
        let hostname = match host {
            Host::Tcp(s) => s.as_bytes(),
            #[cfg(unix)]
            // This would be the place to convert DEFAULT_PGSOCKET_DIR to 'localhost' if it was possible.
            Host::Unix(pathbuf) => pathbuf.as_os_str().as_bytes(),
        };
        let port_string = format!("{}", port).into_bytes();
        // This default is applied by the server, rather than our caller, so we have to apply it here too.
        let dbname = dbname.unwrap_or(user);
        PassfileKey {
            hostname,
            port: port_string,
            dbname: dbname.as_bytes(),
            user: user.as_bytes(),
        }
    }
}

/// A single field (other than the password) in a passfile line.
enum PassfileField {
    Wildcard,
    Bytes(Vec<u8>),
}

impl PassfileField {
    fn accepts(&self, value: &[u8]) -> bool {
        match self {
            PassfileField::Wildcard => true,
            PassfileField::Bytes(b) => (b == value),
        }
    }
}

/// The data from a single passfile line.
struct PassfileEntry {
    hostname: PassfileField,
    port: PassfileField,
    dbname: PassfileField,
    user: PassfileField,
    password: Vec<u8>,
}

impl PassfileEntry {
    fn new(s: &[u8]) -> Result<PassfileEntry, ()> {
        let mut it = s.iter().copied();

        let mut parse_one_field = || {
            let mut value = Vec::new();
            let mut has_any_escape = false;
            while let Some(b) = it.next() {
                if b == b':' {
                    return Ok(match &value[..] {
                        b"*" if !has_any_escape => PassfileField::Wildcard,
                        _ => PassfileField::Bytes(value),
                    });
                } else if b == b'\\' {
                    has_any_escape = true;
                    value.push(it.next().ok_or(())?);
                } else if b == b'\0' {
                    return Err(());
                } else {
                    value.push(b)
                }
            }
            Err(())
        };
        let hostname = parse_one_field()?;
        let port = parse_one_field()?;
        let dbname = parse_one_field()?;
        let user = parse_one_field()?;

        let mut parse_final_field = || {
            let mut value = Vec::new();
            while let Some(b) = it.next() {
                if b == b':' {
                    // Text that looks like an additional field is ignored
                    return Ok(value);
                } else if b == b'\\' {
                    // To be consistent with libpq, if the line ends with a backslash then the backslash is treated as
                    // part of the last field's value.
                    value.push(it.next().unwrap_or(b'\\'))
                } else if b == b'\0' {
                    return Err(());
                } else {
                    value.push(b)
                }
            }
            Ok(value)
        };
        let password = parse_final_field()?;

        Ok(PassfileEntry {
            hostname,
            port,
            dbname,
            user,
            password,
        })
    }
}

/// Removes trailing CR and/or LF from a string.
///
/// Intended to match libpq's pg_strip_crlf().
fn strip_crlf(bb: &[u8]) -> &[u8] {
    for (idx, b) in bb.iter().copied().enumerate().rev() {
        if b != b'\n' && b != b'\r' {
            return &bb[..=idx];
        }
    }
    &bb[..0]
}

/// Searches passfile text for a match.
///
/// Intended to match libpq's behavior closely.
/// If there is an IO error, returns None.
async fn password_for_key<T>(key: &PassfileKey<'_>, buf: T) -> Option<Vec<u8>>
where
    T: tokio::io::AsyncBufReadExt + Unpin,
{
    let mut it = buf.split(b'\n');
    // If we get an io error, just stop reading
    while let Ok(Some(line)) = it.next_segment().await {
        if line.starts_with(&[b'#']) {
            continue;
        }
        let line = strip_crlf(&line);
        if line.is_empty() {
            continue;
        }
        if let Ok(entry) = PassfileEntry::new(line) {
            if entry.hostname.accepts(&key.hostname)
                && entry.port.accepts(&key.port)
                && entry.dbname.accepts(&key.dbname)
                && entry.user.accepts(&key.user)
            {
                if entry.password.is_empty() {
                    // To be consistent with libpq, in this case we need to stop searching the password file, but not
                    // attempt to use the empty password string.
                    return None;
                } else {
                    return Some(entry.password);
                }
            }
        }
    }
    None
}

/// Searches a passfile, returning the password from the first matching line.
///
/// Returns None if:
///  - there is no match
///  - the passfile doesn't exist
///  - the passfile permissions allow any access for group or 'other' (unix only)
///  - there is an error reading the passfile
///  - `config` doesn't have a user set.
///
/// If `config` doesn't have a dbname set, matches the dbname field against the user name.
pub(crate) async fn find_password(
    passfile: &Path,
    config: &Config,
    host: &Host,
    port: u16,
) -> Option<Vec<u8>> {
    // If we don't have a user, this connection can never work anyway
    if let Some(user) = config.get_user() {
        #[cfg(unix)]
        {
            let meta = match fs::metadata(&passfile).await {
                Ok(meta) => meta,
                Err(_) => return None,
            };
            if !meta.is_file() {
                warn!(
                    "password file \"{}\" is not a plain file",
                    &passfile.to_string_lossy()
                );
                return None;
            }
            if (meta.mode() & 0o0077) != 0 {
                warn!(
                    "password file \"{}\" has group or world access; permissions should be u=rw (0600) or less",
                    &passfile.to_string_lossy()
                );
                return None;
            }
        }
        let key = PassfileKey::new(host, port, config.get_dbname(), &user);
        let file = match File::open(&passfile).await {
            Ok(file) => file,
            Err(_) => return None,
        };
        password_for_key(&key, BufReader::new(file)).await
    } else {
        None
    }
}
