import asyncio
import os
import shutil
import signal
import subprocess
import time
import uuid
import aiohttp
import orjson
import redis.asyncio as redis
import uvloop

uvloop.install()

BOT_TOKEN = os.environ["BOT_TOKEN"]
REDIS_URL = os.environ["REDIS_URL"]
NODE_NAME = os.environ.get("NODE_NAME", "worker-node")
RAMDISK = os.environ.get("TMPDIR", "/dev/shm")
LOCAL_API = os.environ.get("LOCAL_BOT_API_URL", "https://api.telegram.org")
API_BASE = f"{LOCAL_API}/bot{BOT_TOKEN}"
PROC_QUEUE = f"bot:processing:{NODE_NAME}"

SHUTDOWN = False
PROCESSED_COUNT = 0
ACTIVE_TASKS = 0

def handle_sigterm(*_):
    global SHUTDOWN
    SHUTDOWN = True

signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)

def get_tailscale_ip() -> str:
    try:
        return subprocess.check_output(["tailscale", "ip", "-4"], text=True).strip()
    except Exception:
        return "N/A"

async def heartbeat_loop(r: redis.Redis):
    ts_ip = get_tailscale_ip()
    while not SHUTDOWN:
        try:
            load_1m = os.getloadavg()[0]
            disk = shutil.disk_usage(RAMDISK)
            ramdisk_free_gb = round(disk.free / (1024 ** 3), 2)
            payload = {
                "node": NODE_NAME,
                "ip": ts_ip,
                "cpu_load": round(load_1m, 2),
                "ramdisk_free_gb": ramdisk_free_gb,
                "active_tasks": ACTIVE_TASKS,
                "processed": PROCESSED_COUNT,
                "last_seen": int(time.time()),
            }
            await r.hset("cluster:nodes", NODE_NAME, orjson.dumps(payload))
        except Exception:
            pass
        await asyncio.sleep(10)

async def acquire_rate_limit(r: redis.Redis):
    # کنترل نرخ ارسال سراسری بین تمام اکانت‌ها (حداکثر 28 پیام در ثانیه)
    while True:
        current = await r.incr("tg:global_ratelimit")
        if current == 1:
            await r.expire("tg:global_ratelimit", 1)
        if current <= 28:
            return
        await asyncio.sleep(0.05)

async def process_task(session: aiohttp.ClientSession, r: redis.Redis, raw_item: bytes):
    global PROCESSED_COUNT, ACTIVE_TASKS
    ACTIVE_TASKS += 1
    update = orjson.loads(raw_item)
    upd_type = update.get("type", "message")
    data = update.get("data", update)

    job_dir = os.path.join(RAMDISK, f"job_{uuid.uuid4().hex}")
    os.makedirs(job_dir, exist_ok=True)

    try:
        if upd_type == "callback_query":
            cb_id = data["id"]
            chat_id = data["message"]["chat"]["id"]
            cb_data = data.get("data", "")

            await acquire_rate_limit(r)
            async with session.post(
                f"{API_BASE}/answerCallbackQuery",
                json={"callback_query_id": cb_id},
            ) as resp:
                await resp.read()

            # محل اجرای منطق دکمه‌های شیشه‌ای داخل مسیر job_dir
            await acquire_rate_limit(r)
            async with session.post(
                f"{API_BASE}/sendMessage",
                json={"chat_id": chat_id, "text": f"[{NODE_NAME}] دکمه پردازش شد: {cb_data}"},
            ) as resp:
                await resp.read()
        else:
            chat_id = data["chat"]["id"]
            text = data.get("text", "")

            # --- محل قرارگیری منطق دانلود و پردازش سنگین ربات داخل مسیر job_dir ---
            reply_text = f"پاسخ از سرور [{NODE_NAME}]\nدرخواست: {text}"

            await acquire_rate_limit(r)
            async with session.post(
                f"{API_BASE}/sendMessage",
                json={"chat_id": chat_id, "text": reply_text},
            ) as resp:
                await resp.read()

        PROCESSED_COUNT += 1
    finally:
        await r.lrem(PROC_QUEUE, 1, raw_item)
        ACTIVE_TASKS -= 1
        shutil.rmtree(job_dir, ignore_errors=True)

async def worker_loop(session: aiohttp.ClientSession, r: redis.Redis):
    while not SHUTDOWN:
        raw_item = await r.brpoplpush("bot:tasks", PROC_QUEUE, timeout=2)
        if raw_item:
            asyncio.create_task(process_task(session, r, raw_item))

async def main():
    r = redis.from_url(REDIS_URL, decode_responses=False)
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=600, use_dns_cache=True)
    print(f"[{NODE_NAME}] Worker Online & Connected to Central Redis.")

    async with aiohttp.ClientSession(connector=connector) as session:
        hb = asyncio.create_task(heartbeat_loop(r))
        # 64 پردازشگر هم‌زمان روی هر سرور برای هندل کردن هزاران کاربر
        workers = [asyncio.create_task(worker_loop(session, r)) for _ in range(64)]
        await asyncio.gather(*workers)

        for _ in range(20):
            if ACTIVE_TASKS == 0:
                break
            await asyncio.sleep(0.5)

        while True:
            leftover = await r.rpoplpush(PROC_QUEUE, "bot:tasks")
            if not leftover:
                break
        hb.cancel()

if __name__ == "__main__":
    asyncio.run(main())
