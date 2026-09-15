#!/bin/sh
set -eu

RETENTION_DAYS="${DATA_RETENTION_DAYS:-30}"

echo "Retention job started - keeping ${RETENTION_DAYS} days, checking once every 24h."

while true; do
  echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') running analytics.cleanup_old_data(${RETENTION_DAYS})"
  psql -h postgres -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -c "SELECT * FROM analytics.cleanup_old_data(${RETENTION_DAYS});"
  sleep 86400
done
