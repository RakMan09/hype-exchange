package com.hypeexchange.common.model;

/**
 * A conversion event (the deepest funnel step). Typically the latest and most
 * out-of-order event in the firehose.
 *
 * @param requestId the originating auction id
 * @param memeId    the converted meme
 * @param bidderId  the bidder credited with the impression
 * @param value     conversion value, integer cents
 * @param ts        event-time of the conversion, epoch millis
 */
public record Conversion(
        String requestId,
        String memeId,
        String bidderId,
        long value,
        long ts) implements EventTimed {
}
