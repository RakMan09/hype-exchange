// Client-side simulator that produces the exact same MetricsSnapshot shape the
// auctioneer streams over SSE. Used only for the always-on static demo (GitHub
// Pages) where no backend is running. It is clearly labelled as simulated in the
// UI; the real backend logic is proven by the test suite and CI, not by this file.

const BIDDERS = ['bidder-1', 'bidder-2', 'bidder-3', 'bidder-4', 'bidder-5', 'bidder-6'];
const MEME_COUNT = 40;
const HOT_MEMES = 4; // a few memes dominate, so some clearly "moon"
const START_BUDGET = 10_000_000;

function randn() {
  // Box-Muller for gentle, natural-looking jitter.
  let u = 0;
  let v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

function clamp(x, lo, hi) {
  return Math.max(lo, Math.min(hi, x));
}

/**
 * Start emitting a simulated metrics snapshot every `intervalMs`.
 * @returns a stop() function.
 */
export function startSimulator(onSnapshot, intervalMs = 1000) {
  const bidderState = new Map(
    BIDDERS.map((id) => [id, { spend: 0, wins: 0, budget: START_BUDGET }])
  );
  const memeState = new Map();
  for (let i = 0; i < MEME_COUNT; i++) {
    memeState.set(`meme-${i}`, { wins: 0, spend: 0, lastWins: 0 });
  }

  let totalAuctions = 0;
  let filledAuctions = 0;

  const tick = () => {
    // Sustained throughput with mild variation.
    const perSec = clamp(1800 + randn() * 250, 800, 3200);
    const batch = Math.round(perSec);
    const fillRate = clamp(0.96 + randn() * 0.01, 0.85, 0.999);
    const filledThisTick = Math.round(batch * fillRate);

    totalAuctions += batch;
    filledAuctions += filledThisTick;

    for (let i = 0; i < filledThisTick; i++) {
      // Weight wins toward the hot memes.
      const meme =
        Math.random() < 0.55
          ? `meme-${Math.floor(Math.random() * HOT_MEMES)}`
          : `meme-${Math.floor(Math.random() * MEME_COUNT)}`;
      const bidder = BIDDERS[Math.floor(Math.random() * BIDDERS.length)];
      const price = Math.round(clamp(400 + randn() * 180, 100, 1200));

      const b = bidderState.get(bidder);
      if (b.budget - price >= 0) {
        b.budget -= price;
        b.spend += price;
        b.wins += 1;
        const m = memeState.get(meme);
        m.wins += 1;
        m.spend += price;
      }
    }

    // Latency: fast median, tail pressing against the 100ms deadline but under it.
    const p50 = Math.round(clamp(2 + Math.abs(randn()) * 2, 1, 12));
    const p99 = Math.round(clamp(74 + randn() * 8, 40, 99));
    const max = Math.round(clamp(p99 + Math.abs(randn()) * 6, p99, 130));

    const topBidders = [...bidderState.entries()]
      .map(([bidderId, s]) => ({
        bidderId,
        spend: s.spend,
        wins: s.wins,
        remainingBudget: s.budget,
      }))
      .sort((a, b) => b.spend - a.spend)
      .slice(0, 10);

    const topMovers = [...memeState.entries()]
      .map(([memeId, s]) => {
        const velocity = s.wins - s.lastWins;
        s.lastWins = s.wins;
        return { memeId, wins: s.wins, spend: s.spend, velocity };
      })
      .filter((m) => m.wins > 0)
      .sort((a, b) => b.wins - a.wins)
      .slice(0, 10);

    onSnapshot({
      ts: Date.now(),
      totalAuctions,
      filledAuctions,
      fillRate: filledAuctions / Math.max(1, totalAuctions),
      auctionsPerSec: perSec,
      p50LatencyMs: p50,
      p99LatencyMs: p99,
      maxLatencyMs: max,
      topBidders,
      topMovers,
    });
  };

  tick();
  const handle = setInterval(tick, intervalMs);
  return () => clearInterval(handle);
}
