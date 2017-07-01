// Copyright 2012-2014 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
use std::str::FromStr;
use hex::FromHex;

pub struct Url {
    pub scheme: String,
    pub user: Option<UserInfo>,
    pub host: String,
    pub port: Option<u16>,
    pub path: Path,
}

pub struct Path {
    pub path: String,
    pub query: Query,
    pub fragment: Option<String>,
}

pub struct UserInfo {
    pub user: String,
    pub pass: Option<String>,
}

pub type Query = Vec<(String, String)>;

impl Url {
    pub fn new(
        scheme: String,
        user: Option<UserInfo>,
        host: String,
        port: Option<u16>,
        path: String,
        query: Query,
        fragment: Option<String>,
    ) -> Url {
        Url {
            scheme: scheme,
            user: user,
            host: host,
            port: port,
            path: Path::new(path, query, fragment),
        }
    }

    pub fn parse(rawurl: &str) -> DecodeResult<Url> {
        // scheme
        let (scheme, rest) = get_scheme(rawurl)?;

        // authority
        let (userinfo, host, port, rest) = get_authority(rest)?;

        // path
        let has_authority = !host.is_empty();
        let (path, rest) = get_path(rest, has_authority)?;

        // query and fragment
        let (query, fragment) = get_query_fragment(rest)?;

        let url = Url::new(
            scheme.to_owned(),
            userinfo,
            host.to_owned(),
            port,
            path,
            query,
            fragment,
        );
        Ok(url)
    }
}

impl Path {
    pub fn new(path: String, query: Query, fragment: Option<String>) -> Path {
        Path {
            path: path,
            query: query,
            fragment: fragment,
        }
    }

    pub fn parse(rawpath: &str) -> DecodeResult<Path> {
        let (path, rest) = get_path(rawpath, false)?;

        // query and fragment
        let (query, fragment) = get_query_fragment(&rest)?;

        Ok(Path {
            path: path,
            query: query,
            fragment: fragment,
        })
    }
}

impl UserInfo {
    #[inline]
    pub fn new(user: String, pass: Option<String>) -> UserInfo {
        UserInfo {
            user: user,
            pass: pass,
        }
    }
}

pub type DecodeResult<T> = Result<T, String>;

pub fn decode_component(container: &str) -> DecodeResult<String> {
    decode_inner(container, false)
}

fn decode_inner(c: &str, full_url: bool) -> DecodeResult<String> {
    let mut out = String::new();
    let mut iter = c.as_bytes().iter().cloned();

    loop {
        match iter.next() {
            Some(b) => {
                match b as char {
                    '%' => {
                        let bytes = match (iter.next(), iter.next()) {
                            (Some(one), Some(two)) => [one, two],
                            _ => {
                                return Err(
                                    "Malformed input: found '%' without two \
                                                    trailing bytes"
                                        .to_owned(),
                                )
                            }
                        };

                        let bytes_from_hex = match Vec::<u8>::from_hex(&bytes) {
                            Ok(b) => b,
                            _ => {
                                return Err(
                                    "Malformed input: found '%' followed by \
                                            invalid hex  values. Character '%' must \
                                            escaped."
                                        .to_owned(),
                                )
                            }
                        };

                        // Only decode some characters if full_url:
                        match bytes_from_hex[0] as char {
                            // gen-delims:
                            ':' | '/' | '?' | '#' | '[' | ']' | '@' | '!' | '$' | '&' | '"' |
                            '(' | ')' | '*' | '+' | ',' | ';' | '=' if full_url => {
                                out.push('%');
                                out.push(bytes[0] as char);
                                out.push(bytes[1] as char);
                            }

                            ch => out.push(ch),
                        }
                    }
                    ch => out.push(ch),
                }
            }
            None => return Ok(out),
        }
    }
}

fn split_char_first(s: &str, c: char) -> (&str, &str) {
    let mut iter = s.splitn(2, c);

    match (iter.next(), iter.next()) {
        (Some(a), Some(b)) => (a, b),
        (Some(a), None) => (a, ""),
        (None, _) => unreachable!(),
    }
}

fn query_from_str(rawquery: &str) -> DecodeResult<Query> {
    let mut query: Query = vec![];
    if !rawquery.is_empty() {
        for p in rawquery.split('&') {
            let (k, v) = split_char_first(p, '=');
            query.push((decode_component(k)?, decode_component(v)?));
        }
    }

    Ok(query)
}

pub fn get_scheme(rawurl: &str) -> DecodeResult<(&str, &str)> {
    for (i, c) in rawurl.chars().enumerate() {
        let result = match c {
            'A'...'Z' | 'a'...'z' => continue,
            '0'...'9' | '+' | '-' | '.' => {
                if i != 0 {
                    continue;
                }

                Err("url: Scheme must begin with a letter.".to_owned())
            }
            ':' => {
                if i == 0 {
                    Err("url: Scheme cannot be empty.".to_owned())
                } else {
                    Ok((&rawurl[0..i], &rawurl[i + 1..rawurl.len()]))
                }
            }
            _ => Err("url: Invalid character in scheme.".to_owned()),
        };

        return result;
    }

    Err("url: Scheme must be terminated with a colon.".to_owned())
}

// returns userinfo, host, port, and unparsed part, or an error
fn get_authority(rawurl: &str) -> DecodeResult<(Option<UserInfo>, &str, Option<u16>, &str)> {
    enum State {
        Start, // starting state
        PassHostPort, // could be in user or port
        Ip6Port, // either in ipv6 host or port
        Ip6Host, // are in an ipv6 host
        InHost, // are in a host - may be ipv6, but don't know yet
        InPort, // are in port
    }

    #[derive(Clone, PartialEq)]
    enum Input {
        Digit, // all digits
        Hex, // digits and letters a-f
        Unreserved, // all other legal characters
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

    let mut colon_count = 0usize;
    let mut pos = 0;
    let mut begin = 2;
    let mut end = len;

    for (i, c) in rawurl.chars().enumerate().skip(2) {
        // deal with input class first
        match c {
            '0'...'9' => (),
            'A'...'F' | 'a'...'f' => {
                if input == Input::Digit {
                    input = Input::Hex;
                }
            }
            'G'...'Z' | 'g'...'z' | '-' | '.' | '_' | '~' | '%' | '&' | '\'' | '(' | ')' |
            '+' | '!' | '*' | ',' | ';' | '=' => input = Input::Unreserved,
            ':' | '@' | '?' | '#' | '/' => {
                // separators, don't change anything
            }
            _ => return Err("Illegal character in authority".to_owned()),
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
                            return Err("Illegal characters in IPv6 address.".to_owned());
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
                            return Err("Illegal characters in authority.".to_owned());
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
                    _ => return Err("Invalid ':' in authority.".to_owned()),
                }
                input = Input::Digit; // reset input class
            }

            '@' => {
                input = Input::Digit; // reset input class
                colon_count = 0; // reset count
                match st {
                    State::Start => {
                        let user = decode_component(&rawurl[begin..i])?;
                        userinfo = Some(UserInfo::new(user, None));
                        st = State::InHost;
                    }
                    State::PassHostPort => {
                        let user = decode_component(&rawurl[begin..pos])?;
                        let pass = decode_component(&rawurl[pos + 1..i])?;
                        userinfo = Some(UserInfo::new(user, Some(pass)));
                        st = State::InHost;
                    }
                    _ => return Err("Invalid '@' in authority.".to_owned()),
                }
                begin = i + 1;
            }

            '?' | '#' | '/' => {
                end = i;
                break;
            }
            _ => (),
        }
    }

    // finish up
    match st {
        State::PassHostPort | State::Ip6Port => {
            if input != Input::Digit {
                return Err("Non-digit characters in port.".to_owned());
            }
            host = &rawurl[begin..pos];
            port = Some(&rawurl[pos + 1..end]);
        }
        State::Ip6Host | State::InHost | State::Start => host = &rawurl[begin..end],
        State::InPort => {
            if input != Input::Digit {
                return Err("Non-digit characters in port.".to_owned());
            }
            port = Some(&rawurl[pos + 1..end]);
        }
    }

    let rest = &rawurl[end..len];
    // If we have a port string, ensure it parses to u16.
    let port = match port {
        None => None,
        opt => {
            match opt.and_then(|p| FromStr::from_str(p).ok()) {
                None => return Err(format!("Failed to parse port: {:?}", port)),
                opt => opt,
            }
        }
    };

    Ok((userinfo, host, port, rest))
}


// returns the path and unparsed part of url, or an error
fn get_path(rawurl: &str, is_authority: bool) -> DecodeResult<(String, &str)> {
    let len = rawurl.len();
    let mut end = len;
    for (i, c) in rawurl.chars().enumerate() {
        match c {
            'A'...'Z' | 'a'...'z' | '0'...'9' | '&' | '\'' | '(' | ')' | '.' | '@' | ':' |
            '%' | '/' | '+' | '!' | '*' | ',' | ';' | '=' | '_' | '-' | '~' => continue,
            '?' | '#' => {
                end = i;
                break;
            }
            _ => return Err("Invalid character in path.".to_owned()),
        }
    }

    if is_authority && end != 0 && !rawurl.starts_with('/') {
        Err(
            "Non-empty path must begin with '/' in presence of authority.".to_owned(),
        )
    } else {
        Ok((decode_component(&rawurl[0..end])?, &rawurl[end..len]))
    }
}

// returns the parsed query and the fragment, if present
fn get_query_fragment(rawurl: &str) -> DecodeResult<(Query, Option<String>)> {
    let (before_fragment, raw_fragment) = split_char_first(rawurl, '#');

    // Parse the fragment if available
    let fragment = match raw_fragment {
        "" => None,
        raw => Some(decode_component(raw)?),
    };

    match before_fragment.chars().next() {
        Some('?') => Ok((query_from_str(&before_fragment[1..])?, fragment)),
        None => Ok((vec![], fragment)),
        _ => Err(format!(
            "Query didn't start with '?': '{}..'",
            before_fragment
        )),
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
