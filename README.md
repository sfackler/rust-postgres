# Materialize fork of Rust-Postgres

This repo serves as a staging area in order to develop and use features of the
rust postgtres client before they are accepted upstream.

Since development on this repo and the upstream one can happen in parallel this
repo adops a branching strategy that keeps both in sync and keeps a tidy
history. Importantly, the release branches are **never** forced-pushed so that
older versions of materialize are always buildable.

## Branching strategy

For every upstream release a local `mz-{version}` branch is created. The latest
such branch should be made the default branch of this repo in the Github
settings.

Whenever a PR is opened it should targed the current release branch (it should
be picked automatically if its set as default on Github).

Whenever a new version is created upstream a new `mz-{version}` branch on this
repo is created, initially pointing at the release commit of the upstream repo.
Then, all the fork-specific work is rebased on top of it. This process gives
the opportunity to prune PRs that have successfully made it to the upstream
repo and keep a clean per-version history.
