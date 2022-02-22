# Materialize fork of Rust-Postgres

This repo serves as a staging area for Materialize patches to the
[rust-postgres] client before they are accepted upstream.

There are no releases from this fork. The [MaterializeInc/materialize]
repository simply pins a recent commit from the `master` branch. Other projects
are welcome to do the same. The `master` branch is never force pushed. Upstream
changes are periodically into `master` via `git merge`.

## Adding a new patch

Develop your patch against the master branch of the upstream [rust-postgres]
project. Open a PR with your changes. If your PR is not merged quickly, open the
same PR against this repository and request a review from a Materialize
engineer.

The long-term goal is to get every patch merged upstream.

## Integrating upstream changes

```shell
git clone https://github.com/MaterializeInc/rust-postgres.git
git remote add upstream https://github.com/sfackler/rust-postgres.git
git checkout master
git pull
git checkout -b integrate-upstream
git fetch upstream
git merge upstream/master
# Resolve any conflicts, then open a PR against this repository with the merge commit.
```

[rust-postgres]: https://github.com/sfackler/rust-postgres
[MaterializeInc/materialize]: https://github.com/MaterializeInc/materialize
