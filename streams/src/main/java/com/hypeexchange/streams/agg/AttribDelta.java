package com.hypeexchange.streams.agg;

/** A single contribution to per-meme attribution: one impression or one click. */
public record AttribDelta(long impressions, long clicks) {

    public static AttribDelta impression() {
        return new AttribDelta(1, 0);
    }

    public static AttribDelta click() {
        return new AttribDelta(0, 1);
    }
}
