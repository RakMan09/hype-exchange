package com.hypeexchange.auctioneer.metrics;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A fixed-size ring buffer of recent latency samples for cheap percentile queries.
 * Recording is lock-free; percentile snapshots copy-and-sort the filled portion.
 * Rare write races on the same slot are acceptable for a metrics reservoir.
 */
public class LatencyReservoir {

    private final long[] samples;
    private final AtomicLong count = new AtomicLong();

    public LatencyReservoir(int size) {
        this.samples = new long[size];
    }

    public void record(long value) {
        int idx = (int) (count.getAndIncrement() % samples.length);
        samples[idx] = value;
    }

    /** @param p percentile in [0,1]; returns 0 when no samples recorded yet. */
    public long percentile(double p) {
        long n = count.get();
        if (n == 0) {
            return 0L;
        }
        int filled = (int) Math.min(n, samples.length);
        long[] copy = Arrays.copyOf(samples, filled);
        Arrays.sort(copy);
        int idx = (int) Math.ceil(p * filled) - 1;
        idx = Math.max(0, Math.min(filled - 1, idx));
        return copy[idx];
    }

    public long max() {
        long n = count.get();
        if (n == 0) {
            return 0L;
        }
        int filled = (int) Math.min(n, samples.length);
        long m = 0;
        for (int i = 0; i < filled; i++) {
            m = Math.max(m, samples[i]);
        }
        return m;
    }
}
