import { useEffect, useRef, useState } from 'react';
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid, ReferenceLine,
} from 'recharts';
import { useEventSource } from './useEventSource.js';

const DEADLINE_MS = 100;
const HISTORY = 60;

const money = (cents) => `$${(cents / 100).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
const pct = (x) => `${(x * 100).toFixed(1)}%`;

function Stat({ label, value, sub, tone }) {
  return (
    <div className={`stat ${tone || ''}`}>
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
      {sub && <div className="stat-sub">{sub}</div>}
    </div>
  );
}

export default function App() {
  const { data, connected } = useEventSource('/stream/metrics');
  const [history, setHistory] = useState([]);
  const lastTs = useRef(0);

  useEffect(() => {
    if (!data || data.ts === lastTs.current) return;
    lastTs.current = data.ts;
    setHistory((h) => [
      ...h.slice(-(HISTORY - 1)),
      {
        t: new Date(data.ts).toLocaleTimeString(),
        p99: data.p99LatencyMs,
        p50: data.p50LatencyMs,
        rate: Math.round(data.auctionsPerSec),
      },
    ]);
  }, [data]);

  const bidders = data?.topBidders ?? [];
  const movers = data?.topMovers ?? [];

  return (
    <div className="app">
      <header>
        <h1>HypeExchange <span className="floor">live trading floor</span></h1>
        <span className={`conn ${connected ? 'on' : 'off'}`}>
          {connected ? 'live' : 'disconnected'}
        </span>
      </header>

      <section className="stats">
        <Stat label="Auctions / sec" value={Math.round(data?.auctionsPerSec ?? 0)} />
        <Stat label="Fill rate" value={pct(data?.fillRate ?? 0)} />
        <Stat label="p50 latency" value={`${data?.p50LatencyMs ?? 0} ms`} />
        <Stat
          label="p99 latency"
          value={`${data?.p99LatencyMs ?? 0} ms`}
          sub={`deadline ${DEADLINE_MS} ms`}
          tone={(data?.p99LatencyMs ?? 0) > DEADLINE_MS ? 'warn' : 'ok'}
        />
        <Stat label="Total auctions" value={(data?.totalAuctions ?? 0).toLocaleString()} />
        <Stat label="Max latency" value={`${data?.maxLatencyMs ?? 0} ms`} />
      </section>

      <section className="grid">
        <div className="card">
          <h2>Auction latency (p50 / p99)</h2>
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={history}>
              <CartesianGrid strokeDasharray="3 3" stroke="#243049" />
              <XAxis dataKey="t" tick={{ fontSize: 10, fill: '#8aa0c6' }} minTickGap={40} />
              <YAxis tick={{ fontSize: 10, fill: '#8aa0c6' }} />
              <Tooltip contentStyle={{ background: '#111a2e', border: '1px solid #243049' }} />
              <ReferenceLine y={DEADLINE_MS} stroke="#f2545b" strokeDasharray="4 4"
                label={{ value: 'deadline', fill: '#f2545b', fontSize: 10 }} />
              <Line type="monotone" dataKey="p99" stroke="#f6c445" dot={false} strokeWidth={2} />
              <Line type="monotone" dataKey="p50" stroke="#4cc9f0" dot={false} strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="card">
          <h2>Spend by bidder</h2>
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={bidders.map((b) => ({ ...b, spendUsd: b.spend / 100 }))}>
              <CartesianGrid strokeDasharray="3 3" stroke="#243049" />
              <XAxis dataKey="bidderId" tick={{ fontSize: 10, fill: '#8aa0c6' }} />
              <YAxis tick={{ fontSize: 10, fill: '#8aa0c6' }} />
              <Tooltip contentStyle={{ background: '#111a2e', border: '1px solid #243049' }}
                formatter={(v) => `$${Number(v).toLocaleString()}`} />
              <Bar dataKey="spendUsd" fill="#7b6cf6" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="card">
          <h2>Top movers <span className="muted">what's mooning</span></h2>
          <table>
            <thead>
              <tr><th>Meme</th><th>Wins</th><th>Spend</th><th>Velocity</th></tr>
            </thead>
            <tbody>
              {movers.map((m) => (
                <tr key={m.memeId}>
                  <td>{m.memeId}</td>
                  <td>{m.wins.toLocaleString()}</td>
                  <td>{money(m.spend)}</td>
                  <td className={m.velocity > 0 ? 'up' : ''}>
                    {m.velocity > 0 ? `▲ ${m.velocity}` : m.velocity}
                  </td>
                </tr>
              ))}
              {movers.length === 0 && <tr><td colSpan="4" className="muted">waiting for events…</td></tr>}
            </tbody>
          </table>
        </div>

        <div className="card">
          <h2>Bidder leaderboard</h2>
          <table>
            <thead>
              <tr><th>Bidder</th><th>Wins</th><th>Spend</th><th>Budget left</th></tr>
            </thead>
            <tbody>
              {bidders.map((b) => (
                <tr key={b.bidderId}>
                  <td>{b.bidderId}</td>
                  <td>{b.wins.toLocaleString()}</td>
                  <td>{money(b.spend)}</td>
                  <td>{b.remainingBudget >= 9.2e18 ? '∞' : money(b.remainingBudget)}</td>
                </tr>
              ))}
              {bidders.length === 0 && <tr><td colSpan="4" className="muted">waiting for events…</td></tr>}
            </tbody>
          </table>
        </div>
      </section>

      <footer>
        HypeExchange · deadline-bounded RTB auctions · metrics via SSE from the auctioneer
      </footer>
    </div>
  );
}
