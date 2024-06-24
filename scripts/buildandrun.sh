#!/usr/bin/env sh
set -e

go build ./cmd/spirit

if [ -n "$REPLICA_DSN" ]; then
  ./spirit --replica-dsn "$REPLICA_DSN" --host $HOST --username $USERNAME --password $PASSWORD --database=$DATABASE --table=$TABLE
else
  ./spirit --replica-dsn "$REPLICA_DSN" --host $HOST --username $USERNAME --password $PASSWORD --database=$DATABASE --table=$TABLE
fi
