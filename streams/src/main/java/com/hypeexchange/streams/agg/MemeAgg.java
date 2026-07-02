package com.hypeexchange.streams.agg;

import com.hypeexchange.common.model.AuctionResult;

/** Windowed accumulator for per-meme auction metrics. */
public record MemeAgg(long auctions, long fills, long spend) {

    public static MemeAgg empty() {
        return new MemeAgg(0, 0, 0);
    }

    public MemeAgg add(AuctionResult r) {
        if (r == null) {
            return this;
        }
        return new MemeAgg(
                auctions + 1,
                fills + (r.filled() ? 1 : 0),
                spend + (r.filled() ? r.clearingPrice() : 0));
    }

    public long avgClearingPrice() {
        return fills == 0 ? 0 : spend / fills;
    }

    public double fillRate() {
        return auctions == 0 ? 0.0 : (double) fills / auctions;
    }
}
