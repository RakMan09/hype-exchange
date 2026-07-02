"""HypeExchange traffic generator.

Fires synthetic hype events at the auctioneer at a target rate. Each event opens an
auction; the auctioneer fans out to bidders, clears, and emits the firehose.

Usage:
    python gen/firehose.py [--url http://localhost:8080/auctions]
                           [--rate 200] [--duration 60] [--memes 50]

--rate is auctions/sec (approximate); --duration 0 runs until interrupted.
"""
import argparse
import asyncio
import random
import time
import uuid

import httpx

CATEGORIES = ["meme", "sound", "creator", "dance"]


def make_event(meme_pool):
    meme = random.choice(meme_pool)
    return {
        "id": str(uuid.uuid4()),
        "memeId": meme,
        "category": random.choice(CATEGORIES),
        "floorPrice": random.choice([100, 200, 300, 500]),
        "ts": int(time.time() * 1000),
    }


async def worker(client, url, queue, stats):
    while True:
        event = await queue.get()
        if event is None:
            queue.task_done()
            return
        try:
            resp = await client.post(url, json=event)
            if resp.status_code == 200:
                body = resp.json()
                stats["ok"] += 1
                if body.get("filled"):
                    stats["filled"] += 1
            else:
                stats["err"] += 1
        except Exception:
            stats["err"] += 1
        finally:
            queue.task_done()


async def run(args):
    # Zipf-ish popularity: a few memes dominate, so some clearly "moon".
    meme_pool = [f"meme-{i}" for i in range(args.memes)]
    hot = meme_pool[: max(1, args.memes // 10)]
    weighted_pool = meme_pool + hot * 8

    stats = {"ok": 0, "filled": 0, "err": 0}
    queue: asyncio.Queue = asyncio.Queue(maxsize=args.rate * 2)

    limits = httpx.Limits(max_connections=args.concurrency,
                          max_keepalive_connections=args.concurrency)
    async with httpx.AsyncClient(timeout=2.0, limits=limits) as client:
        workers = [asyncio.create_task(worker(client, args.url, queue, stats))
                   for _ in range(args.concurrency)]

        interval = 1.0 / args.rate if args.rate > 0 else 0
        start = time.time()
        last_report = start
        n = 0
        try:
            while args.duration == 0 or time.time() - start < args.duration:
                await queue.put(make_event(weighted_pool))
                n += 1
                if interval:
                    await asyncio.sleep(interval)
                now = time.time()
                if now - last_report >= 2.0:
                    elapsed = now - start
                    print(f"sent={n} ok={stats['ok']} filled={stats['filled']} "
                          f"err={stats['err']} rate={n / elapsed:.0f}/s")
                    last_report = now
        except KeyboardInterrupt:
            pass
        finally:
            await queue.join()
            for _ in workers:
                await queue.put(None)
            await asyncio.gather(*workers, return_exceptions=True)

    elapsed = max(1e-6, time.time() - start)
    print(f"done: sent={n} ok={stats['ok']} filled={stats['filled']} "
          f"err={stats['err']} avg_rate={n / elapsed:.0f}/s")


def main():
    parser = argparse.ArgumentParser(description="HypeExchange traffic generator")
    parser.add_argument("--url", default="http://localhost:8080/auctions")
    parser.add_argument("--rate", type=int, default=200, help="auctions/sec")
    parser.add_argument("--duration", type=int, default=60, help="seconds (0 = forever)")
    parser.add_argument("--memes", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=64)
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
