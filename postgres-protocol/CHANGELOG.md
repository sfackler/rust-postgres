# Change Log

## [Unreleased]

## [v0.4.0] - 2019-05-03

### Added

* Added channel binding support to SCRAM authentication API.

### Changed

* Passwords are no longer required to be UTF8 strings.
* `types::array_to_sql` now automatically computes the required flags and no longer takes a has_nulls parameter.

## Older

Look at the [release tags] for information about older releases.

[Unreleased]: https://github.com/sfackler/rust-postgres/compare/postgres-protocol-v0.4.0...master
[v0.4.0]: https://github.com/sfackler/rust-postgres/compare/postgres-protocol-v0.3.2...postgres-protocol-v0.4.0
[release tags]: https://github.com/sfackler/rust-postgres/releases
