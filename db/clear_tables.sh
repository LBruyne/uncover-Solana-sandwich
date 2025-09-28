#!/usr/bin/env bash

set -a
source ../watcher/.env
set +a

HOST=$CLICKHOUSE_ADDR
USER=$CLICKHOUSE_USERNAME
PASSWORD=$CLICKHOUSE_PASSWORD
DATABASE=$CLICKHOUSE_DATABASE

CH_HOST="$HOST"
CH_PORT="9000"
if [[ "$HOST" == *:* ]]; then
  CH_HOST="${HOST%%:*}"
  CH_PORT="${HOST##*:}"
fi

CHC=(clickhouse-client --host "$CH_HOST" --port "$CH_PORT" --user "$USER")
[[ -n "$PASSWORD" ]] && CHC+=(--password "$PASSWORD")

if [[ $# -lt 1 ]]; then
  echo "usage: $0 table1 [table2 ...]"
  echo "example: $0 solwich.sandwiches solwich.sandwich_txs"
  echo "or：  $0 sandwiches sandwich_txs"
  exit 1
fi

for T in "$@"; do
  if [[ "$T" != *.* ]]; then
    T="${DATABASE}.${T}"
  fi
  echo "Dropping table: $T"
  "${CHC[@]}" --query "DROP TABLE IF EXISTS \`${T%%.*}\`.\`${T##*.}\` SYNC"
done

echo "Done."