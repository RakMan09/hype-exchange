package com.hypeexchange.common.model;

/**
 * A downstream "invest" action on a meme (a user puts money behind the hype).
 *
 * @param requestId the originating auction id
 * @param memeId    the meme invested in
 * @param bidderId  the bidder credited with the impression
 * @param amount    invested amount, integer cents
 * @param ts        event-time of the invest, epoch millis
 */
public record Invest(
        String requestId,
        String memeId,
        String bidderId,
        long amount,
        long ts) implements EventTimed {
}
