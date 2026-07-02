package com.hypeexchange.streams.model;

/**
 * Per-meme windowed attribution metrics emitted to {@code metrics-attribution}.
 *
 * @param memeId           the meme
 * @param windowStart      window start, epoch millis
 * @param windowEnd        window end, epoch millis
 * @param impressions      impressions in the window
 * @param attributedClicks clicks attributed to impressions in this window
 * @param ctr              attributedClicks / impressions (0 when no impressions)
 */
public record MemeAttribution(
        String memeId,
        long windowStart,
        long windowEnd,
        long impressions,
        long attributedClicks,
        double ctr) {
}
