#!/bin/sh
set -e

echo "[entrypoint] Initializing database..."
python database.py

echo "[entrypoint] Waiting for DNS..."
for i in $(seq 1 30); do
  python - <<'EOF' >/dev/null 2>&1 && break
import socket
socket.gethostbyname("discord.com")
EOF
  sleep 1
done

echo "[entrypoint] Starting bot..."
exec python main.py

