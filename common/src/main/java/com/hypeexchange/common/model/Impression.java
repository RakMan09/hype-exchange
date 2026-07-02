package com.hypeexchange.common.model;

/**
 * An impression served to the winning bidder. Emitted immediately after a filled
 * auction and the anchor event for downstream attribution.
 *
 * @param requestId the auction id (attribution key)
 * @param memeId    the meme shown
 * @param bidderId  the winning bidder
 * @param price     clearing price paid, integer cents
 * @param ts        event-time of the impression, epoch millis
 */
public record Impression(
        String requestId,
        String memeId,
        String bidderId,
        long price,
        long ts) {
}
