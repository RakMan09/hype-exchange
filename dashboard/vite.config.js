import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// The auctioneer serves the SSE endpoints on :8080. Proxy them in dev so the
// browser can use same-origin EventSource URLs like `/stream/metrics`.
const AUCTIONEER = process.env.AUCTIONEER_URL || 'http://localhost:8080';

// Base path: '/' for local/dev and container serving; set VITE_BASE (e.g.
// '/hype-exchange/') for GitHub Pages project sites.
const BASE = process.env.VITE_BASE || '/';

export default defineConfig({
  base: BASE,
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/stream': { target: AUCTIONEER, changeOrigin: true },
      '/auctions': { target: AUCTIONEER, changeOrigin: true },
    },
  },
});
