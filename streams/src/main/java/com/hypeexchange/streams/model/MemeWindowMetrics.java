package com.hypeexchange.streams.model;

/**
 * Per-meme windowed metrics emitted to {@code metrics-meme}.
 *
 * @param memeId            the meme
 * @param windowStart       window start, epoch millis (inclusive)
 * @param windowEnd         window end, epoch millis (exclusive)
 * @param auctions          auctions seen in the window (a "hype velocity" proxy)
 * @param fills             auctions that produced a winner
 * @param spend             total clearing price across fills, integer cents
 * @param avgClearingPrice  eCPM proxy: spend / fills (0 when no fills)
 * @param fillRate          fills / auctions
 */
public record MemeWindowMetrics(
        String memeId,
        long windowStart,
        long windowEnd,
        long auctions,
        long fills,
        long spend,
        long avgClearingPrice,
        double fillRate) {
}
