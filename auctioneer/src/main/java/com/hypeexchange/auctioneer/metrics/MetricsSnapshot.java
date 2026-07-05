package com.hypeexchange.auctioneer.metrics;

import java.util.List;

/**
 * A point-in-time view of live auction metrics, pushed to the dashboard over SSE.
 */
public record MetricsSnapshot(
        long ts,
        long totalAuctions,
        long filledAuctions,
        double fillRate,
        double auctionsPerSec,
        long p50LatencyMs,
        long p95LatencyMs,
        long p99LatencyMs,
        long maxLatencyMs,
        long deadlineMs,
        double deadlineComplianceRate,
        long totalSpend,
        long avgClearingPrice,
        double avgBidsPerAuction,
        double stragglerDropRate,
        List<BidderSpend> topBidders,
        List<MemeMovement> topMovers) {

    /** Spend and win count for a single bidder. */
    public record BidderSpend(String bidderId, long spend, long wins, long remainingBudget) {
    }

    /** Activity for a single meme (a "mover" on the trading floor). */
    public record MemeMovement(String memeId, long wins, long spend, long velocity) {
    }
}
