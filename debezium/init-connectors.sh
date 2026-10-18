#!/bin/sh
set -eu
CONNECT_URL=http://kafka-connect:8083
# Extract only the config object; the password stays an env-provider reference.
sed -n '/"config": {/,$p' /connectors/finance-db-connector.json \
  | sed '1s/.*"config": //' | sed '$d' >/tmp/config.json
curl --fail-with-body --retry 20 --retry-connrefused --retry-delay 3 \
  -sS -X PUT "$CONNECT_URL/connectors/finance-db-connector/config" \
  -H 'Content-Type: application/json' --data-binary @/tmp/config.json >/dev/null
for attempt in $(seq 1 60); do
  status=$(curl -fsS "$CONNECT_URL/connectors/finance-db-connector/status")
  if echo "$status" | grep -q '"state":"FAILED"'; then
    echo 'Debezium connector failed; inspect kafka-connect logs.' >&2
    exit 1
  fi
  if [ "$(echo "$status" | grep -o '"state":"RUNNING"' | wc -l)" -ge 2 ]; then
    echo 'Debezium connector and task are running.'
    exit 0
  fi
  sleep 2
done
echo 'Timed out waiting for Debezium task.' >&2
exit 1
