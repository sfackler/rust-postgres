# Change Log

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
