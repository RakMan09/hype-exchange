package com.hypeexchange.streams.agg;

import com.hypeexchange.common.model.AuctionResult;

/** Windowed accumulator for per-bidder spend metrics. */
public record BidderAgg(long wins, long spend) {

    public static BidderAgg empty() {
        return new BidderAgg(0, 0);
    }

    public BidderAgg add(AuctionResult r) {
        if (r == null || !r.filled()) {
            return this;
        }
        return new BidderAgg(wins + 1, spend + r.clearingPrice());
    }

    public long avgClearingPrice() {
        return wins == 0 ? 0 : spend / wins;
    }
}
