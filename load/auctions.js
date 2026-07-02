import http from 'k6/http';
import { check } from 'k6';
import { Rate, Trend } from 'k6/metrics';

// HypeExchange load test: drive auctions through the auctioneer and assert the
// deadline holds under load (p99 decision latency below the deadline) with a
// stable fill rate.
//
// Run:
//   k6 run load/auctions.js
//   k6 run -e URL=http://localhost:8080/auctions -e RATE=5000 -e DURATION=60s load/auctions.js

const URL = __ENV.URL || 'http://localhost:8080/auctions';
const RATE = parseInt(__ENV.RATE || '2000', 10);        // auctions/sec target
const DURATION = __ENV.DURATION || '30s';
const MEMES = parseInt(__ENV.MEMES || '50', 10);

const fillRate = new Rate('filled_rate');
const decisionLatency = new Trend('decision_latency_ms', true);

const CATEGORIES = ['meme', 'sound', 'creator', 'dance'];
const FLOORS = [100, 200, 300, 500];

export const options = {
  scenarios: {
    auctions: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.max(50, Math.ceil(RATE / 10)),
      maxVUs: Math.max(200, Math.ceil(RATE / 2)),
    },
  },
  thresholds: {
    // The whole point: keep p99 decision latency under the 100ms deadline.
    http_req_duration: ['p(99)<100', 'p(95)<90'],
    filled_rate: ['rate>0.4'],
    checks: ['rate>0.98'],
  },
};

function rand(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

export default function () {
  const body = JSON.stringify({
    memeId: `meme-${Math.floor(Math.random() * MEMES)}`,
    category: rand(CATEGORIES),
    floorPrice: rand(FLOORS),
    ts: Date.now(),
  });

  const res = http.post(URL, body, { headers: { 'Content-Type': 'application/json' } });

  const ok = check(res, {
    'status 200': (r) => r.status === 200,
    'has result': (r) => r.body && r.body.length > 0,
  });

  if (ok && res.status === 200) {
    try {
      const result = res.json();
      fillRate.add(result.filled === true);
      if (typeof result.latencyMs === 'number') {
        decisionLatency.add(result.latencyMs);
      }
    } catch (_) {
      // ignore parse errors
    }
  }
}
