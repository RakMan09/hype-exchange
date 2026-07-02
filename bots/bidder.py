"""A single HypeExchange bidder bot.

Exposes ``POST /bid`` which the auctioneer calls during fan-out. The bot returns a
bid (or abstains) using a simple randomized, category-aware strategy. It can also
inject artificial latency so you can watch the auctioneer's deadline drop stragglers.

Environment:
    BIDDER_ID          identifier reported in bids (default: derived from port)
    BID_PROBABILITY    chance the bot bids at all (default 0.85)
    BASE_PRICE         center of the bid price distribution, in cents (default 500)
    PRICE_JITTER       +/- jitter around the base price, in cents (default 400)
    SLOW_PROBABILITY   chance of a deliberately slow response (default 0.05)
    SLOW_MS            delay applied to slow responses, in ms (default 250)
"""
import os
import random

from fastapi import FastAPI, Response, status

BIDDER_ID = os.getenv("BIDDER_ID", f"bidder-{os.getenv('PORT', '0')}")
BID_PROBABILITY = float(os.getenv("BID_PROBABILITY", "0.85"))
BASE_PRICE = int(os.getenv("BASE_PRICE", "500"))
PRICE_JITTER = int(os.getenv("PRICE_JITTER", "400"))
SLOW_PROBABILITY = float(os.getenv("SLOW_PROBABILITY", "0.05"))
SLOW_MS = int(os.getenv("SLOW_MS", "250"))

# Per-category multipliers make some bots favour some categories -> richer dynamics.
CATEGORY_WEIGHTS = {
    "meme": 1.2,
    "sound": 1.0,
    "creator": 1.4,
    "dance": 0.9,
}

app = FastAPI(title=f"HypeExchange bidder {BIDDER_ID}")

import asyncio  # noqa: E402  (imported after config for readability)
import time  # noqa: E402


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "bidderId": BIDDER_ID}


@app.post("/bid")
async def bid(req: dict, response: Response):
    # Occasionally respond slowly to exercise the auctioneer's hard deadline.
    if random.random() < SLOW_PROBABILITY:
        await asyncio.sleep(SLOW_MS / 1000.0)

    # Sometimes abstain entirely.
    if random.random() > BID_PROBABILITY:
        response.status_code = status.HTTP_204_NO_CONTENT
        return None

    weight = CATEGORY_WEIGHTS.get(req.get("category", ""), 1.0)
    floor = int(req.get("floorPrice", 0))
    price = int((BASE_PRICE + random.randint(-PRICE_JITTER, PRICE_JITTER)) * weight)
    # Never bid below the floor; a sensible bidder respects the reserve.
    price = max(price, floor)

    return {
        "requestId": req.get("id"),
        "bidderId": BIDDER_ID,
        "price": price,
        "ts": int(time.time() * 1000),
    }


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "9101"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
