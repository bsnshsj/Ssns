#!/usr/bin/env bash
set -e

set -a
[ -f /home/Neotenet/workspace/.env ] && source /home/Neotenet/workspace/.env
set +a

export TMPDIR=/dev/shm
export PYTHONUNBUFFERED=1
export PYTHONOPTIMIZE=2

if [ ! -d "/home/Neotenet/venv" ]; then
  python3 -m venv /home/Neotenet/venv
  /home/Neotenet/venv/bin/pip install -q --upgrade pip
  /home/Neotenet/venv/bin/pip install -q uvloop orjson aiodns brotli cryptg aiohttp redis
  if [ -f requirements.txt ]; then
    /home/Neotenet/venv/bin/pip install -q -r requirements.txt
  fi
fi

if [ -x "./bin/telegram-bot-api" ]; then
  export PATH="$(pwd)/bin:$PATH"
fi

if [ -n "$API_ID" ] && [ -n "$API_HASH" ] && command -v telegram-bot-api >/dev/null 2>&1; then
  mkdir -p /dev/shm/bot-api
  telegram-bot-api \
    --api-id="$API_ID" \
    --api-hash="$API_HASH" \
    --local \
    --http-port=8081 \
    --dir=/dev/shm/bot-api \
    --temp-dir=/dev/shm/bot-api/tmp \
    --max-webhook-connections=10000 >/dev/null 2>&1 &
  export LOCAL_BOT_API_URL="http://127.0.0.1:8081"
fi

exec /home/Neotenet/venv/bin/python worker.py
