# Change Log

## v0.6.0 - 2020-12-25

### Changed

* Upgraded `bytes`, `hmac`, and `rand`.

### Added

* Added `escape::{escape_literal, escape_identifier}`.

## v0.5.3 - 2020-10-17

### Changed

* Upgraded `base64` and `hmac`.

## v0.5.2 - 2020-07-06

### Changed

* Upgraded `hmac` and `sha2`.

## v0.5.1 - 2020-03-17

### Changed

* Upgraded `base64` to 0.12.

## v0.5.0 - 2019-12-23

### Changed

* `frontend::Message` is now a true non-exhaustive enum.

## v0.5.0-alpha.2 - 2019-11-27

### Changed

* Upgraded `bytes` to 0.5.

## v0.5.0-alpha.1 - 2019-10-14

### Changed

* Frontend messages and types now serialize to `BytesMut` rather than `Vec<u8>`.

## v0.4.1 - 2019-06-29

### Added

* Added `backend::Framed` to minimally parse the structure of backend messages.

## v0.4.0 - 2019-03-05

### Added

* Added channel binding support to SCRAM authentication API.

### Changed

* Passwords are no longer required to be UTF8 strings.
* `types::array_to_sql` now automatically computes the required flags and no longer takes a has_nulls parameter.

## Older

Look at the [release tags] for information about older releases.

[release tags]: https://github.com/sfackler/rust-postgres/releases
