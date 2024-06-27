#!/usr/bin/env bash
set -e

go build ./cmd/spirit

params=(--host="$HOST" --username="$USERNAME" --password="$PASSWORD" --database="$DATABASE" --table="$TABLE")

if [ -n "$REPLICA_DSN" ]; then
  params+=(--replica-dsn="$REPLICA_DSN")
fi

./spirit "${params[@]}"
