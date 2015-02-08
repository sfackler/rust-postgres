#!/bin/bash

set -o errexit -o nounset

rev=$(git rev-parse --short HEAD)

git clone --branch gh-pages "https://$GH_TOKEN@github.com/${TRAVIS_REPO_SLUG}.git" deploy_docs
cd deploy_docs

git config user.name "Steven Fackler"
git config user.email "sfackler@gmail.com"

rm -rf docs
mv ../target/docs .

git add -A .
git commit -m "rebuild pages at ${rev}"
git push
