import { useEffect, useState } from 'react';
import { startSimulator } from './simulator.js';

/**
 * Provides the live metrics snapshot from one of two sources:
 *   - the auctioneer SSE endpoint (real backend), or
 *   - the client-side simulator (static demo with no backend).
 *
 * Demo mode is selected by a build flag (VITE_DEMO=true, used for the GitHub Pages
 * build) or a `?demo=1` URL parameter. When not in demo mode it connects to SSE and,
 * if that connection never opens (no backend reachable), it transparently falls back
 * to the simulator so the page is never blank.
 */
export function useMetrics(sseUrl = '/stream/metrics') {
  const [data, setData] = useState(null);
  const [connected, setConnected] = useState(false);
  const [simulated, setSimulated] = useState(false);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const forceDemo = import.meta.env.VITE_DEMO === 'true' || params.get('demo') === '1';
    const forceLive = params.get('live') === '1';

    let stopSim = null;
    let es = null;
    let fallbackTimer = null;

    const runSimulator = () => {
      if (stopSim) return;
      setSimulated(true);
      setConnected(true);
      stopSim = startSimulator((snapshot) => setData(snapshot));
    };

    if (forceDemo && !forceLive) {
      runSimulator();
      return () => stopSim && stopSim();
    }

    es = new EventSource(sseUrl);
    es.onopen = () => {
      setConnected(true);
      if (fallbackTimer) clearTimeout(fallbackTimer);
    };
    es.onmessage = (evt) => {
      try {
        setData(JSON.parse(evt.data));
        setSimulated(false);
      } catch {
        /* ignore malformed frames */
      }
    };
    es.onerror = () => setConnected(false);

    // If the backend never opens the stream, fall back to the simulator.
    fallbackTimer = setTimeout(() => {
      if (!data && es && es.readyState !== EventSource.OPEN) {
        es.close();
        runSimulator();
      }
    }, 2500);

    return () => {
      if (fallbackTimer) clearTimeout(fallbackTimer);
      if (es) es.close();
      if (stopSim) stopSim();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sseUrl]);

  return { data, connected, simulated };
}
