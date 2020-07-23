#!/bin/bash
source scripts/ci_prepare.sh
mix test $1
docker rm --force "myxql-ci" || true
