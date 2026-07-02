import { useEffect, useRef, useState } from 'react';

/**
 * Subscribe to a Server-Sent Events endpoint and expose the latest parsed JSON
 * message plus a connection flag. Reconnects automatically via the browser's
 * built-in EventSource retry.
 */
export function useEventSource(url) {
  const [data, setData] = useState(null);
  const [connected, setConnected] = useState(false);
  const esRef = useRef(null);

  useEffect(() => {
    const es = new EventSource(url);
    esRef.current = es;
    es.onopen = () => setConnected(true);
    es.onerror = () => setConnected(false);
    es.onmessage = (evt) => {
      try {
        setData(JSON.parse(evt.data));
      } catch {
        /* ignore malformed frames */
      }
    };
    return () => es.close();
  }, [url]);

  return { data, connected };
}
