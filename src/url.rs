// Copyright 2012-2014 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::fmt;
use std::str::FromStr;
use std::path::BytesContainer;
use std::num;
use std::str;

#[derive(Clone, PartialEq, Eq)]
pub struct Url {
    pub scheme: String,
    pub user: Option<UserInfo>,
    pub host: String,
    pub port: Option<u16>,
    pub path: Path,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Path {
    pub path: String,
    pub query: Query,
    pub fragment: Option<String>
}

#[derive(Clone, PartialEq, Eq)]
pub struct UserInfo {
    pub user: String,
    pub pass: Option<String>
}

pub type Query = Vec<(String, String)>;

impl Url {
    pub fn new(scheme: String,
               user: Option<UserInfo>,
               host: String,
               port: Option<u16>,
               path: String,
               query: Query,
               fragment: Option<String>)
               -> Url {
        Url {
            scheme: scheme,
            user: user,
            host: host,
            port: port,
            path: Path::new(path, query, fragment)
        }
    }

    pub fn parse(rawurl: &str) -> DecodeResult<Url> {
        // scheme
        let (scheme, rest) = try!(get_scheme(rawurl));

        // authority
        let (userinfo, host, port, rest) = try!(get_authority(rest));

        // path
        let has_authority = host.len() > 0;
        let (path, rest) = try!(get_path(rest, has_authority));

        // query and fragment
        let (query, fragment) = try!(get_query_fragment(rest));

        let url = Url::new(scheme.to_string(),
                            userinfo,
                            host.to_string(),
                            port,
                            path,
                            query,
                            fragment);
        Ok(url)
    }
}

impl Path {
    pub fn new(path: String,
               query: Query,
               fragment: Option<String>)
               -> Path {
        Path {
            path: path,
            query: query,
            fragment: fragment,
        }
    }

    pub fn parse(rawpath: &str) -> DecodeResult<Path> {
        let (path, rest) = try!(get_path(rawpath, false));

        // query and fragment
        let (query, fragment) = try!(get_query_fragment(rest.as_slice()));

        Ok(Path{ path: path, query: query, fragment: fragment })
    }
}

impl UserInfo {
    #[inline]
    pub fn new(user: String, pass: Option<String>) -> UserInfo {
        UserInfo { user: user, pass: pass }
    }
}

pub type DecodeResult<T> = Result<T, String>;

pub fn decode_component<T: BytesContainer>(container: T) -> DecodeResult<String> {
    decode_inner(container, false)
}

fn decode_inner<T: BytesContainer>(c: T, full_url: bool) -> DecodeResult<String> {
    let mut out = String::new();
    let mut iter = c.container_as_bytes().iter().map(|&b| b);

    loop {
        match iter.next() {
            Some(b) => match b as char {
                '%' => {
                    let bytes = match (iter.next(), iter.next()) {
                        (Some(one), Some(two)) => [one as u8, two as u8],
                        _ => return Err(format!("Malformed input: found '%' \
                                                without two trailing bytes")),
                    };

                    // Only decode some characters if full_url:
                    match num::from_str_radix::<usize>(str::from_utf8(&bytes).unwrap(), 16).unwrap() as u8 as char {
                        // gen-delims:
                        ':' | '/' | '?' | '#' | '[' | ']' | '@' |

                        // sub-delims:
                        '!' | '$' | '&' | '"' | '(' | ')' | '*' |
                        '+' | ',' | ';' | '='
                            if full_url => {
                            out.push('%');
                            out.push(bytes[0] as char);
                            out.push(bytes[1] as char);
                        }

                        ch => out.push(ch)
                    }
                }
                ch => out.push(ch)
            },
            None => return Ok(out),
        }
    }
}

fn split_char_first(s: &str, c: char) -> (&str, &str) {
    let mut iter = s.splitn(1, c);

    match (iter.next(), iter.next()) {
        (Some(a), Some(b)) => (a, b),
        (Some(a), None) => (a, ""),
        (None, _) => unreachable!(),
    }
}

impl fmt::Debug for UserInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.pass {
            Some(ref pass) => write!(f, "{}:{}@", self.user, *pass),
            None => write!(f, "{}@", self.user),
        }
    }
}

fn query_from_str(rawquery: &str) -> DecodeResult<Query> {
    let mut query: Query = vec!();
    if !rawquery.is_empty() {
        for p in rawquery.split('&') {
            let (k, v) = split_char_first(p, '=');
            query.push((try!(decode_component(k)),
                        try!(decode_component(v))));
        }
    }

    Ok(query)
}

pub fn get_scheme(rawurl: &str) -> DecodeResult<(&str, &str)> {
    for (i,c) in rawurl.chars().enumerate() {
        let result = match c {
            'A' ... 'Z'
            | 'a' ... 'z' => continue,
            '0' ... '9' | '+' | '-' | '.' => {
                if i != 0 { continue }

                Err("url: Scheme must begin with a letter.".to_string())
            }
            ':' => {
                if i == 0 {
                    Err("url: Scheme cannot be empty.".to_string())
                } else {
                    Ok((&rawurl[0..i], &rawurl[i+1..rawurl.len()]))
                }
            }
            _ => Err("url: Invalid character in scheme.".to_string()),
        };

        return result;
    }

    Err("url: Scheme must be terminated with a colon.".to_string())
}

// returns userinfo, host, port, and unparsed part, or an error
fn get_authority(rawurl: &str) ->
    DecodeResult<(Option<UserInfo>, &str, Option<u16>, &str)> {
    enum State {
        Start, // starting state
        PassHostPort, // could be in user or port
        Ip6Port, // either in ipv6 host or port
        Ip6Host, // are in an ipv6 host
        InHost, // are in a host - may be ipv6, but don't know yet
        InPort // are in port
    }

    #[derive(Clone, PartialEq)]
    enum Input {
        Digit, // all digits
        Hex, // digits and letters a-f
        Unreserved // all other legal characters
    }

    if !rawurl.starts_with("//") {
        // there is no authority.
        return Ok((None, "", None, rawurl));
    }

    let len = rawurl.len();
    let mut st = State::Start;
    let mut input = Input::Digit; // most restricted, start here.

    let mut userinfo = None;
    let mut host = "";
    let mut port = None;

    let mut colon_count = 0us;
    let mut pos = 0;
    let mut begin = 2;
    let mut end = len;

    for (i,c) in rawurl.chars().enumerate()
                               // ignore the leading '//' handled by early return
                               .skip(2) {
        // deal with input class first
        match c {
            '0' ... '9' => (),
            'A' ... 'F'
            | 'a' ... 'f' => {
                if input == Input::Digit {
                    input = Input::Hex;
                }
            }
            'G' ... 'Z'
            | 'g' ... 'z'
            | '-' | '.' | '_' | '~' | '%'
            | '&' |'\'' | '(' | ')' | '+'
            | '!' | '*' | ',' | ';' | '=' => input = Input::Unreserved,
            ':' | '@' | '?' | '#' | '/' => {
                // separators, don't change anything
            }
            _ => return Err("Illegal character in authority".to_string()),
        }

        // now process states
        match c {
          ':' => {
            colon_count += 1;
            match st {
              State::Start => {
                pos = i;
                st = State::PassHostPort;
              }
              State::PassHostPort => {
                // multiple colons means ipv6 address.
                if input == Input::Unreserved {
                    return Err(
                        "Illegal characters in IPv6 address.".to_string());
                }
                st = State::Ip6Host;
              }
              State::InHost => {
                pos = i;
                if input == Input::Unreserved {
                    // must be port
                    host = &rawurl[begin..i];
                    st = State::InPort;
                } else {
                    // can't be sure whether this is an ipv6 address or a port
                    st = State::Ip6Port;
                }
              }
              State::Ip6Port => {
                if input == Input::Unreserved {
                    return Err("Illegal characters in authority.".to_string());
                }
                st = State::Ip6Host;
              }
              State::Ip6Host => {
                if colon_count > 7 {
                    host = &rawurl[begin..i];
                    pos = i;
                    st = State::InPort;
                }
              }
              _ => return Err("Invalid ':' in authority.".to_string()),
            }
            input = Input::Digit; // reset input class
          }

          '@' => {
            input = Input::Digit; // reset input class
            colon_count = 0; // reset count
            match st {
              State::Start => {
                let user = rawurl[begin..i].to_string();
                userinfo = Some(UserInfo::new(user, None));
                st = State::InHost;
              }
              State::PassHostPort => {
                let user = rawurl[begin..pos].to_string();
                let pass = rawurl[pos+1..i].to_string();
                userinfo = Some(UserInfo::new(user, Some(pass)));
                st = State::InHost;
              }
              _ => return Err("Invalid '@' in authority.".to_string()),
            }
            begin = i+1;
          }

          '?' | '#' | '/' => {
            end = i;
            break;
          }
          _ => ()
        }
    }

    // finish up
    match st {
      State::Start => host = &rawurl[begin..end],
      State::PassHostPort
      | State::Ip6Port => {
        if input != Input::Digit {
            return Err("Non-digit characters in port.".to_string());
        }
        host = &rawurl[begin..pos];
        port = Some(&rawurl[pos+1..end]);
      }
      State::Ip6Host
      | State::InHost => host = &rawurl[begin..end],
      State::InPort => {
        if input != Input::Digit {
            return Err("Non-digit characters in port.".to_string());
        }
        port = Some(&rawurl[pos+1..end]);
      }
    }

    let rest = &rawurl[end..len];
    // If we have a port string, ensure it parses to u16.
    let port = match port {
        None => None,
        opt => match opt.and_then(|p| FromStr::from_str(p).ok()) {
            None => return Err(format!("Failed to parse port: {:?}", port)),
            opt => opt
        }
    };

    Ok((userinfo, host, port, rest))
}


// returns the path and unparsed part of url, or an error
fn get_path(rawurl: &str, is_authority: bool) -> DecodeResult<(String, &str)> {
    let len = rawurl.len();
    let mut end = len;
    for (i,c) in rawurl.chars().enumerate() {
        match c {
          'A' ... 'Z'
          | 'a' ... 'z'
          | '0' ... '9'
          | '&' |'\'' | '(' | ')' | '.'
          | '@' | ':' | '%' | '/' | '+'
          | '!' | '*' | ',' | ';' | '='
          | '_' | '-' | '~' => continue,
          '?' | '#' => {
            end = i;
            break;
          }
          _ => return Err("Invalid character in path.".to_string())
        }
    }

    if is_authority && end != 0 && !rawurl.starts_with("/") {
        Err("Non-empty path must begin with \
            '/' in presence of authority.".to_string())
    } else {
        Ok((try!(decode_component(&rawurl[0..end])), &rawurl[end..len]))
    }
}

// returns the parsed query and the fragment, if present
fn get_query_fragment(rawurl: &str) -> DecodeResult<(Query, Option<String>)> {
    let (before_fragment, raw_fragment) = split_char_first(rawurl, '#');

    // Parse the fragment if available
    let fragment = match raw_fragment {
        "" => None,
        raw => Some(try!(decode_component(raw)))
    };

    match before_fragment.slice_shift_char() {
        Some(('?', rest)) => Ok((try!(query_from_str(rest)), fragment)),
        None => Ok((vec!(), fragment)),
        _ => Err(format!("Query didn't start with '?': '{}..'", before_fragment)),
    }
}

impl FromStr for Url {
    type Err = String;
    fn from_str(s: &str) -> Result<Url, String> {
        Url::parse(s)
    }
}

impl FromStr for Path {
    type Err = String;
    fn from_str(s: &str) -> Result<Path, String> {
        Path::parse(s)
    }
}

