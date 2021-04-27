//! Support for reading password files.
//!
//! Requires the `runtime` Cargo feature.

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

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
            // libpq translates DEFAULT_PGSOCKET_DIR to 'localhost' here, but we can't do the same that because we don't
            // know what DEFAULT_PGSOCKET_DIR is.
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

/// The data from a single passfile line.
struct PassfileEntry {
    hostname: Vec<u8>,
    port: Vec<u8>,
    dbname: Vec<u8>,
    user: Vec<u8>,
    password: Vec<u8>,
}

impl PassfileEntry {
    fn new(s: &[u8]) -> Result<PassfileEntry, ()> {
        let mut it = s.iter().copied();
        let mut parse_one_field = |allow_eol| {
            let mut value = Vec::new();
            while let Some(b) = it.next() {
                if b == b':' {
                    return Ok(value);
                } else if b == b'\\' {
                    // To be consistent with libpq, if the line ends with a backslash then the backslash is treated as
                    // part of the last field's value.
                    value.push(it.next().unwrap_or(b'\\'))
                } else {
                    value.push(b)
                }
            }
            if allow_eol {
                Ok(value)
            } else {
                Err(())
            }
        };

        Ok(PassfileEntry {
            hostname: parse_one_field(false)?,
            port: parse_one_field(false)?,
            dbname: parse_one_field(false)?,
            user: parse_one_field(false)?,
            password: parse_one_field(true)?,
        })
    }
}

fn field_matches(search_value: &[u8], file_value: &[u8]) -> bool {
    if file_value == [b'*'] {
        return true;
    }
    file_value == search_value
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
            if field_matches(key.hostname, &entry.hostname)
                && field_matches(&key.port, &entry.port)
                && field_matches(key.dbname, &entry.dbname)
                && field_matches(key.user, &entry.user)
            {
                if entry.password.is_empty() {
                    // To be consistent with libpq, in this case we need to
                    // stop searching the password file, but not attempt to
                    // use the empty password string.
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
///  - there is an error reading the passfile
///  - `config` doesn't have a user set.
///
/// If `config` doesn't have a dbname set, matches the dbname field against the user name.
///
/// Intended to match libpq's behavior closely.
///
/// Unlike libpq, doesn't attempt to convert a `host` set to the default socket path into `"localhost"` for matching
/// purposes. So passfiles using `localhost` to supply passwords for unix socket connections won't work.
///
/// Unlike libpq, doesn't enforce any rules on the passfile's permissions.
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
