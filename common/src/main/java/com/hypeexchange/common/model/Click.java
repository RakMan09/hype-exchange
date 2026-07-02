package com.hypeexchange.common.model;

/**
 * A click on a served impression. Arrives after the impression, often late and
 * out of order, and must be attributed back to the impression's window.
 *
 * @param requestId the auction id this click attributes to
 * @param memeId    the meme clicked
 * @param bidderId  the bidder that won the impression
 * @param ts        event-time of the click, epoch millis
 */
public record Click(
        String requestId,
        String memeId,
        String bidderId,
        long ts) {
}
