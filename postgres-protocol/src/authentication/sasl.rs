//! SASL-based authentication support.

use base64;
use generic_array::GenericArray;
use generic_array::typenum::U32;
use hmac::{Hmac, Mac};
use sha2::{Sha256, Digest};
use std::fmt::Write;
use std::io;
use std::iter;
use std::mem;
use std::str;
use rand::{OsRng, Rng};
use stringprep;

const NONCE_LENGTH: usize = 24;

/// The identifier of the SCRAM-SHA-256 SASL authentication mechanism.
pub const SCRAM_SHA_256: &'static str = "SCRAM-SHA-256";

// since postgres passwords are not required to exclude saslprep-prohibited
// characters or even be valid UTF8, we run saslprep if possible and otherwise
// return the raw password.
fn normalize(pass: &[u8]) -> Vec<u8> {
    let pass = match str::from_utf8(pass) {
        Ok(pass) => pass,
        Err(_) => return pass.to_vec(),
    };

    match stringprep::saslprep(pass) {
        Ok(pass) => pass.into_owned().into_bytes(),
        Err(_) => pass.as_bytes().to_vec(),
    }
}

fn hi(str: &[u8], salt: &[u8], i: u32) -> GenericArray<u8, U32> {
    let mut hmac = Hmac::<Sha256>::new(str)
        .expect("HMAC is able to accept all key sizes");
    hmac.input(salt);
    hmac.input(&[0, 0, 0, 1]);
    let mut prev = hmac.result().code();

    let mut hi = GenericArray::<u8, U32>::clone_from_slice(&prev);

    for _ in 1..i {
        let mut hmac = Hmac::<Sha256>::new(str).expect("already checked above");
        hmac.input(prev.as_slice());
        prev = hmac.result().code();

        for (hi, prev) in hi.iter_mut().zip(prev) {
            *hi ^= prev;
        }
    }

    hi
}

enum State {
    Update { nonce: String, password: Vec<u8> },
    Finish {
        salted_password: GenericArray<u8, U32>,
        auth_message: String,
    },
    Done,
}

/// A type which handles the client side of the SCRAM-SHA-256 authentication process.
///
/// During the authentication process, if the backend sends an `AuthenticationSASL` message which
/// includes `SCRAM-SHA-256` as an authentication mechanism, this type can be used.
///
/// After a `ScramSha256` is constructed, the buffer returned by the `message()` method should be
/// sent to the backend in a `SASLInitialResponse` message along with the mechanism name.
///
/// The server will reply with an `AuthenticationSASLContinue` message. Its contents should be
/// passed to the `update()` method, after which the buffer returned by the `message()` method
/// should be sent to the backend in a `SASLResponse` message.
///
/// The server will reply with an `AuthenticationSASLFinal` message. Its contents should be passed
/// to the `finish()` method, after which the authentication process is complete.
pub struct ScramSha256 {
    message: String,
    state: State,
}

#[allow(missing_docs)]
impl ScramSha256 {
    /// Constructs a new instance which will use the provided password for authentication.
    pub fn new(password: &[u8]) -> io::Result<ScramSha256> {
        let mut rng = OsRng::new()?;
        let nonce = (0..NONCE_LENGTH)
            .map(|_| {
                let mut v = rng.gen_range(0x21u8, 0x7e);
                if v == 0x2c {
                    v = 0x7e
                }
                v as char
            })
            .collect::<String>();

        ScramSha256::new_inner(password, nonce)
    }

    fn new_inner(password: &[u8], nonce: String) -> io::Result<ScramSha256> {
        // the docs say to use pg_same_as_startup_message as the username, but
        // psql uses an empty string, so we'll go with that.
        let message = format!("n,,n=,r={}", nonce);

        let password = normalize(password);

        Ok(ScramSha256 {
            message: message,
            state: State::Update {
                nonce: nonce,
                password: password,
            },
        })
    }

    /// Returns the message which should be sent to the backend in an `SASLResponse` message.
    pub fn message(&self) -> &[u8] {
        if let State::Done = self.state {
            panic!("invalid SCRAM state");
        }
        self.message.as_bytes()
    }

    /// Updates the state machine with the response from the backend.
    ///
    /// This should be called when an `AuthenticationSASLContinue` message is received.
    pub fn update(&mut self, message: &[u8]) -> io::Result<()> {
        let (client_nonce, password) = match mem::replace(&mut self.state, State::Done) {
            State::Update { nonce, password } => (nonce, password),
            _ => return Err(io::Error::new(io::ErrorKind::Other, "invalid SCRAM state")),
        };

        let message = str::from_utf8(message).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidInput, e)
        })?;

        let parsed = Parser::new(message).server_first_message()?;

        if !parsed.nonce.starts_with(&client_nonce) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid nonce"));
        }

        let salt = match base64::decode(parsed.salt) {
            Ok(salt) => salt,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
        };

        let salted_password = hi(&password, &salt, parsed.iteration_count);

        let mut hmac = Hmac::<Sha256>::new(&salted_password)
            .expect("HMAC is able to accept all key sizes");
        hmac.input(b"Client Key");
        let client_key = hmac.result().code();

        let mut hash = Sha256::default();
        hash.input(client_key.as_slice());
        let stored_key = hash.result();

        self.message.clear();
        write!(&mut self.message, "c=biws,r={}", parsed.nonce).unwrap();

        let auth_message = format!("n=,r={},{},{}", client_nonce, message, self.message);

        let mut hmac = Hmac::<Sha256>::new(&stored_key)
            .expect("HMAC is able to accept all key sizes");
        hmac.input(auth_message.as_bytes());
        let client_signature = hmac.result();

        let mut client_proof = GenericArray::<u8, U32>::clone_from_slice(&client_key);
        for (proof, signature) in client_proof.iter_mut().zip(client_signature.code()) {
            *proof ^= signature;
        }

        write!(&mut self.message, ",p={}", base64::encode(&*client_proof)).unwrap();

        self.state = State::Finish {
            salted_password: salted_password,
            auth_message: auth_message,
        };
        Ok(())
    }

    /// Finalizes the authentication process.
    ///
    /// This should be called when the backend sends an `AuthenticationSASLFinal` message.
    /// Authentication has only succeeded if this method returns `Ok(())`.
    pub fn finish(&mut self, message: &[u8]) -> io::Result<()> {
        let (salted_password, auth_message) = match mem::replace(&mut self.state, State::Done) {
            State::Finish {
                salted_password,
                auth_message,
            } => (salted_password, auth_message),
            _ => return Err(io::Error::new(io::ErrorKind::Other, "invalid SCRAM state")),
        };

        let message = str::from_utf8(message).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidInput, e)
        })?;

        let parsed = Parser::new(message).server_final_message()?;

        let verifier = match parsed {
            ServerFinalMessage::Error(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("SCRAM error: {}", e),
                ))
            }
            ServerFinalMessage::Verifier(verifier) => verifier,
        };

        let verifier = match base64::decode(verifier) {
            Ok(verifier) => verifier,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
        };

        let mut hmac = Hmac::<Sha256>::new(&salted_password)
            .expect("HMAC is able to accept all key sizes");
        hmac.input(b"Server Key");
        let server_key = hmac.result();

        let mut hmac = Hmac::<Sha256>::new(&server_key.code())
            .expect("HMAC is able to accept all key sizes");
        hmac.input(auth_message.as_bytes());
        hmac.verify(&verifier).map_err(|_| io::Error::new(
            io::ErrorKind::InvalidInput,
            "SCRAM verification error",
        ))
    }
}

struct Parser<'a> {
    s: &'a str,
    it: iter::Peekable<str::CharIndices<'a>>,
}

impl<'a> Parser<'a> {
    fn new(s: &'a str) -> Parser<'a> {
        Parser {
            s: s,
            it: s.char_indices().peekable(),
        }
    }

    fn eat(&mut self, target: char) -> io::Result<()> {
        match self.it.next() {
            Some((_, c)) if c == target => Ok(()),
            Some((i, c)) => {
                let m = format!(
                    "unexpected character at byte {}: expected `{}` but got `{}",
                    i,
                    target,
                    c
                );
                Err(io::Error::new(io::ErrorKind::InvalidInput, m))
            }
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            )),
        }
    }

    fn take_while<F>(&mut self, f: F) -> io::Result<&'a str>
    where
        F: Fn(char) -> bool,
    {
        let start = match self.it.peek() {
            Some(&(i, _)) => i,
            None => return Ok(""),
        };

        loop {
            match self.it.peek() {
                Some(&(_, c)) if f(c) => {
                    self.it.next();
                }
                Some(&(i, _)) => return Ok(&self.s[start..i]),
                None => return Ok(&self.s[start..]),
            }
        }
    }

    fn printable(&mut self) -> io::Result<&'a str> {
        self.take_while(|c| match c {
            '\x21'...'\x2b' | '\x2d'...'\x7e' => true,
            _ => false,
        })
    }

    fn nonce(&mut self) -> io::Result<&'a str> {
        self.eat('r')?;
        self.eat('=')?;
        self.printable()
    }

    fn base64(&mut self) -> io::Result<&'a str> {
        self.take_while(|c| match c {
            'a'...'z' | 'A'...'Z' | '0'...'9' | '/' | '+' | '=' => true,
            _ => false,
        })
    }

    fn salt(&mut self) -> io::Result<&'a str> {
        self.eat('s')?;
        self.eat('=')?;
        self.base64()
    }

    fn posit_number(&mut self) -> io::Result<u32> {
        let n = self.take_while(|c| match c {
            '0'...'9' => true,
            _ => false,
        })?;
        n.parse().map_err(
            |e| io::Error::new(io::ErrorKind::InvalidInput, e),
        )
    }

    fn iteration_count(&mut self) -> io::Result<u32> {
        self.eat('i')?;
        self.eat('=')?;
        self.posit_number()
    }

    fn eof(&mut self) -> io::Result<()> {
        match self.it.peek() {
            Some(&(i, _)) => {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unexpected trailing data at byte {}", i),
                ))
            }
            None => Ok(()),
        }
    }

    fn server_first_message(&mut self) -> io::Result<ServerFirstMessage<'a>> {
        let nonce = self.nonce()?;
        self.eat(',')?;
        let salt = self.salt()?;
        self.eat(',')?;
        let iteration_count = self.iteration_count()?;
        self.eof()?;

        Ok(ServerFirstMessage {
            nonce: nonce,
            salt: salt,
            iteration_count: iteration_count,
        })
    }

    fn value(&mut self) -> io::Result<&'a str> {
        self.take_while(|c| match c {
            '\0' | '=' | ',' => false,
            _ => true,
        })
    }

    fn server_error(&mut self) -> io::Result<Option<&'a str>> {
        match self.it.peek() {
            Some(&(_, 'e')) => {}
            _ => return Ok(None),
        }

        self.eat('e')?;
        self.eat('=')?;
        self.value().map(Some)
    }

    fn verifier(&mut self) -> io::Result<&'a str> {
        self.eat('v')?;
        self.eat('=')?;
        self.base64()
    }

    fn server_final_message(&mut self) -> io::Result<ServerFinalMessage<'a>> {
        let message = match self.server_error()? {
            Some(error) => ServerFinalMessage::Error(error),
            None => ServerFinalMessage::Verifier(self.verifier()?),
        };
        self.eof()?;
        Ok(message)
    }
}

struct ServerFirstMessage<'a> {
    nonce: &'a str,
    salt: &'a str,
    iteration_count: u32,
}

enum ServerFinalMessage<'a> {
    Error(&'a str),
    Verifier(&'a str),
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_server_first_message() {
        let message = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";
        let message = Parser::new(message).server_first_message().unwrap();
        assert_eq!(message.nonce, "fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j");
        assert_eq!(message.salt, "QSXCR+Q6sek8bf92");
        assert_eq!(message.iteration_count, 4096);
    }

    // recorded auth exchange from psql
    #[test]
    fn exchange() {
        let password = "foobar";
        let nonce = "9IZ2O01zb9IgiIZ1WJ/zgpJB";

        let client_first = "n,,n=,r=9IZ2O01zb9IgiIZ1WJ/zgpJB";
        let server_first = "r=9IZ2O01zb9IgiIZ1WJ/zgpJBjx/oIRLs02gGSHcw1KEty3eY,s=fs3IXBy7U7+IvVjZ,i\
                            =4096";
        let client_final = "c=biws,r=9IZ2O01zb9IgiIZ1WJ/zgpJBjx/oIRLs02gGSHcw1KEty3eY,p=AmNKosjJzS3\
                            1NTlQYNs5BTeQjdHdk7lOflDo5re2an8=";
        let server_final = "v=U+ppxD5XUKtradnv8e2MkeupiA8FU87Sg8CXzXHDAzw=";

        let mut scram = ScramSha256::new_inner(password.as_bytes(), nonce.to_string()).unwrap();
        assert_eq!(str::from_utf8(scram.message()).unwrap(), client_first);

        scram.update(server_first.as_bytes()).unwrap();
        assert_eq!(str::from_utf8(scram.message()).unwrap(), client_final);

        scram.finish(server_final.as_bytes()).unwrap();
    }
}
