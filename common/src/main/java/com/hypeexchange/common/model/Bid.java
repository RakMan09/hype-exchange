package com.hypeexchange.common.model;

/**
 * A single bidder's response to a {@link BidRequest}.
 *
 * @param requestId the auction this bid belongs to
 * @param bidderId  the responding bidder
 * @param price     bid price in integer cents
 * @param ts        time the bid was produced, epoch millis
 */
public record Bid(
        String requestId,
        String bidderId,
        long price,
        long ts) implements EventTimed {
}
