package com.hypeexchange.streams.model;

/**
 * Per-bidder windowed metrics emitted to {@code metrics-bidder}.
 *
 * @param bidderId          the bidder
 * @param windowStart       window start, epoch millis (inclusive)
 * @param windowEnd         window end, epoch millis (exclusive)
 * @param wins              auctions won in the window
 * @param spend             total spend (clearing prices), integer cents
 * @param avgClearingPrice  spend / wins (0 when no wins)
 */
public record BidderWindowMetrics(
        String bidderId,
        long windowStart,
        long windowEnd,
        long wins,
        long spend,
        long avgClearingPrice) {
}
