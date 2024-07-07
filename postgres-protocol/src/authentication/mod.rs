//! Authentication protocol support.
use md5::{Digest, Md5};
use sm3::sm3;

pub mod sasl;

/// Hashes authentication information in a way suitable for use in response
/// to an `AuthenticationMd5Password` message.
///
/// The resulting string should be sent back to the database in a
/// `PasswordMessage` message.
#[inline]
pub fn md5_hash(username: &[u8], password: &[u8], salt: [u8; 4]) -> String {
    let mut md5 = Md5::new();
    md5.update(password);
    md5.update(username);
    let output = md5.finalize_reset();
    md5.update(format!("{:x}", output));
    md5.update(salt);
    format!("md5{:x}", md5.finalize())
}

/// Hashes authentication information in a way suitable for use in response
/// to an `AuthenticationSm3Password` message.
///
/// The resulting string should be sent back to the database in a
/// `PasswordMessage` message.
#[inline]
pub fn sm3_hash(username: &[u8], password: &[u8], salt: [u8; 4]) -> String {
    let mut sm3 = Sm3::new();
    sm3.update(password);
    sm3.update(username);
    let output = sm3.finalize_reset();
    sm3.update(format!("{:x}", output));
    sm3.update(salt);
    format!("sm3{:x}", sm3.finalize())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn md5() {
        let username = b"md5_user";
        let password = b"password";
        let salt = [0x2a, 0x3d, 0x8f, 0xe0];

        assert_eq!(
            md5_hash(username, password, salt),
            "md562af4dd09bbb41884907a838a3233294"
        );
    }

    #[test]
    fn sm3() {
        let username = b"sm3_user";
        let password = b"password";
        let salt = [0x2a, 0x3d, 0x8f, 0xe0];

        assert_eq!(
            sm3_hash(username, password, salt),
            "sm360f647fba99d2a5375a6dc448fd58604c80f67757939595e2601e4885de8dd3b"
        );
    }
}
