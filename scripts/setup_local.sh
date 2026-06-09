#!/usr/bin/env bash
# Bring up the local mock stack (Kafka + ES + SFTP) and prepare keys.
#
# Idempotent: re-running just makes sure containers are up and keys exist.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL="$ROOT/local"
SFTP_DIR="$LOCAL/sftp"
KEY_DIR="$LOCAL/keys"

mkdir -p "$KEY_DIR" "$SFTP_DIR/ssh_host_keys" "$SFTP_DIR/upload"

# --- ETL client key pair -----------------------------------------------------
if [ ! -f "$KEY_DIR/id_ed25519" ]; then
    echo "==> generating etl client ed25519 key"
    ssh-keygen -t ed25519 -N "" -f "$KEY_DIR/id_ed25519" -C "etl-local" >/dev/null
fi
install -m 600 "$KEY_DIR/id_ed25519.pub" "$SFTP_DIR/etl_authorized_keys"

# --- Stable SFTP host keys ---------------------------------------------------
# Persisted on the host so known_hosts stays valid across docker compose down/up.
for kt in rsa ed25519; do
    keyfile="$SFTP_DIR/ssh_host_keys/ssh_host_${kt}_key"
    if [ ! -f "$keyfile" ]; then
        echo "==> generating sftp host key ($kt)"
        ssh-keygen -t "$kt" -N "" -f "$keyfile" -C "sftp-local-host" >/dev/null
        chmod 600 "$keyfile"
    fi
done

# --- Bring up stack ----------------------------------------------------------
echo "==> docker compose up -d"
docker compose -f "$ROOT/docker-compose.yml" up -d

# --- Wait for health ---------------------------------------------------------
echo "==> waiting for elasticsearch..."
for _ in $(seq 1 60); do
    if curl -sf http://localhost:9200/_cluster/health >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
curl -sf http://localhost:9200/_cluster/health?pretty || { echo "elasticsearch did not come up"; exit 1; }

echo "==> waiting for sftp..."
for _ in $(seq 1 30); do
    if nc -z localhost 2222 2>/dev/null; then
        break
    fi
    sleep 1
done

echo "==> waiting for kafka..."
for _ in $(seq 1 60); do
    if docker exec etl-kafka kafka-broker-api-versions \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
docker exec etl-kafka kafka-broker-api-versions \
    --bootstrap-server localhost:9092 >/dev/null 2>&1 \
    || { echo "kafka did not come up"; exit 1; }

# --- Capture host key into known_hosts --------------------------------------
echo "==> writing known_hosts"
ssh-keyscan -p 2222 -H -t ed25519,rsa localhost > "$KEY_DIR/known_hosts" 2>/dev/null
chmod 600 "$KEY_DIR/known_hosts"

# --- Create the control topic (autocreate is on, but be explicit) -----------
echo "==> creating control topic etl.control"
docker exec etl-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic etl.control \
    --partitions 1 --replication-factor 1 >/dev/null

cat <<EOF

mock stack is up.

  Kafka:          localhost:9092
  Elasticsearch:  http://localhost:9200
  SFTP:           sftp://etl@localhost:2222   (key: $KEY_DIR/id_ed25519)
  Kibana (opt):   docker compose --profile ui up -d kibana   → http://localhost:5601

next:
  cp .env.local .env
  .venv/bin/python scripts/seed.py
  .venv/bin/python -m etl
EOF
