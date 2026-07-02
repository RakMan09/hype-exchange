package com.hypeexchange.streams.agg;

/** Windowed accumulator for per-meme impression/click counts. */
public record AttribAgg(long impressions, long clicks) {

    public static AttribAgg empty() {
        return new AttribAgg(0, 0);
    }

    public AttribAgg add(AttribDelta d) {
        return new AttribAgg(impressions + d.impressions(), clicks + d.clicks());
    }

    public double ctr() {
        return impressions == 0 ? 0.0 : (double) clicks / impressions;
    }
}
