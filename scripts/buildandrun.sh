#!/usr/bin/env sh
set -e

go build ./cmd/spirit
./spirit --host "mysql:3306" --database=test --table=t1