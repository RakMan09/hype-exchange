package com.hypeexchange.common.model;

import java.util.List;

/**
 * Outcome of a single auction.
 *
 * @param requestId     the auction id
 * @param memeId        the meme that was auctioned
 * @param winnerId      winning bidder, or {@code null} if the auction was unfilled
 * @param clearingPrice price the winner pays in integer cents (0 when unfilled)
 * @param participants  number of bids that arrived before the deadline
 * @param latencyMs     wall-clock time to resolve the auction
 * @param filled        whether a winner was chosen
 * @param auctionType   clearing rule that was applied
 * @param droppedBids   bids that arrived after the deadline and were dropped
 * @param ts            event-time of the auction (mirrors the request ts)
 */
public record AuctionResult(
        String requestId,
        String memeId,
        String winnerId,
        long clearingPrice,
        int participants,
        long latencyMs,
        boolean filled,
        AuctionType auctionType,
        int droppedBids,
        long ts) implements EventTimed {

    public static AuctionResult unfilled(String requestId, String memeId, int participants,
                                         long latencyMs, AuctionType type, int droppedBids, long ts) {
        return new AuctionResult(requestId, memeId, null, 0L, participants, latencyMs, false,
                type, droppedBids, ts);
    }

    /** Convenience view of the winner as a list (empty when unfilled). */
    public List<String> winners() {
        return filled ? List.of(winnerId) : List.of();
    }
}
