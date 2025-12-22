#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SQLITE_DB="${PUBLIC_SEARCH_DB_PATH:-"$ROOT_DIR/data/db/public-search.sqlite"}"
CH_TABLE="${CLICKHOUSE_METRICS_TABLE:-message_metrics}"
CH_CONTAINER="${CLICKHOUSE_CONTAINER:-ch}"
ENV_FILE="${ENV_FILE:-"$ROOT_DIR/.env"}"

ensure_env_var() {
  local file="$1"
  local key="$2"
  local value="$3"
  if [[ ! -f "$file" ]]; then
    touch "$file"
  fi
  if grep -Eq "^${key}=" "$file"; then
    return
  fi
  printf "\n%s=%s\n" "$key" "$value" >> "$file"
}

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "sqlite3 not found"
  exit 1
fi

if [[ ! "$CH_TABLE" =~ ^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$ ]]; then
  echo "Invalid CLICKHOUSE_METRICS_TABLE: $CH_TABLE"
  exit 1
fi

DOCKER="docker"
if ! $DOCKER ps >/dev/null 2>&1; then
  if command -v sudo >/dev/null 2>&1; then
    DOCKER="sudo docker"
  fi
fi

if ! $DOCKER ps >/dev/null 2>&1; then
  echo "Docker is not доступен. Проверь права на /var/run/docker.sock."
  exit 1
fi

if ! $DOCKER ps --format '{{.Names}}' | grep -qx "$CH_CONTAINER"; then
  if $DOCKER ps -a --format '{{.Names}}' | grep -qx "$CH_CONTAINER"; then
    $DOCKER start "$CH_CONTAINER" >/dev/null
  else
    echo "ClickHouse container '$CH_CONTAINER' not found. Запусти контейнер и попробуй снова."
    exit 1
  fi
fi

if ! sqlite3 "$SQLITE_DB" "SELECT name FROM sqlite_master WHERE type='table' AND name='message_metrics';" | grep -qx message_metrics; then
  echo "Таблица message_metrics не найдена в SQLite: $SQLITE_DB"
  exit 1
fi

$DOCKER exec -i "$CH_CONTAINER" clickhouse-client --query "
CREATE TABLE IF NOT EXISTS ${CH_TABLE} (
  run_id UInt64,
  chat_id Int64,
  message_id Int64,
  ts DateTime,
  view_count Nullable(UInt32),
  forward_count Nullable(UInt32),
  reply_count Nullable(UInt32),
  reactions_total Nullable(UInt32),
  reactions_paid Nullable(UInt32),
  reactions_free Nullable(UInt32)
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (chat_id, message_id, ts, run_id)
"

SQLITE_QUERY=$(cat <<'SQL'
SELECT
  run_id,
  chat_id,
  message_id,
  datetime(ts, 'unixepoch') AS ts,
  CASE WHEN view_count IS NULL THEN '\N' ELSE view_count END AS view_count,
  CASE WHEN forward_count IS NULL THEN '\N' ELSE forward_count END AS forward_count,
  CASE WHEN reply_count IS NULL THEN '\N' ELSE reply_count END AS reply_count,
  CASE WHEN reactions_total IS NULL THEN '\N' ELSE reactions_total END AS reactions_total,
  CASE WHEN reactions_paid IS NULL THEN '\N' ELSE reactions_paid END AS reactions_paid,
  CASE WHEN reactions_free IS NULL THEN '\N' ELSE reactions_free END AS reactions_free
FROM message_metrics
ORDER BY run_id, chat_id, message_id;
SQL
)

sqlite3 "$SQLITE_DB" -header -separator $'\t' "$SQLITE_QUERY" \
  | $DOCKER exec -i "$CH_CONTAINER" clickhouse-client --query \
    "INSERT INTO ${CH_TABLE} FORMAT TabSeparatedWithNames"

SQLITE_COUNT=$(sqlite3 "$SQLITE_DB" "SELECT COUNT(*) FROM message_metrics;")
CH_COUNT=$($DOCKER exec -i "$CH_CONTAINER" clickhouse-client --query "SELECT count() FROM ${CH_TABLE}")

ensure_env_var "$ENV_FILE" "CLICKHOUSE_URL" "http://localhost:8123"
ensure_env_var "$ENV_FILE" "CLICKHOUSE_METRICS_TABLE" "$CH_TABLE"

echo "Готово. SQLite rows: ${SQLITE_COUNT}, ClickHouse rows: ${CH_COUNT}"
echo "Обновлён env: ${ENV_FILE}"
