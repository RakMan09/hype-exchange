package com.hypeexchange.common.model;

/**
 * A hype event that opens an auction. Emitted by the traffic generator and fanned
 * out to bidders by the auctioneer.
 *
 * @param id         unique request id (also the auction id)
 * @param memeId     the trending meme / sound / creator being auctioned
 * @param category   coarse category used by bidder strategies
 * @param floorPrice reserve price in integer cents; bids below this are ignored
 * @param ts         event-time of the hype event, epoch millis
 */
public record BidRequest(
        String id,
        String memeId,
        String category,
        long floorPrice,
        long ts) {
}
