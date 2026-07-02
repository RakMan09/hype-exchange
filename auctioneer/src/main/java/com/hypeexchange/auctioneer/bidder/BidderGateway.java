package com.hypeexchange.auctioneer.bidder;

import com.hypeexchange.common.model.Bid;
import com.hypeexchange.common.model.BidRequest;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Fans a bid request out to the configured bidders and collects the bids that
 * arrive before the deadline. Implementations must guarantee that a slow bidder
 * can never stall the auction beyond {@code deadline}.
 */
public interface BidderGateway {

    /** Number of bidders that will be contacted (used to compute drop counts). */
    int bidderCount();

    /**
     * Collect bids, hard-cutting at the deadline.
     *
     * @param request  the bid request to send
     * @param deadline aggregate deadline for the whole fan-out
     * @return the bids that arrived in time (stragglers are dropped)
     */
    Mono<java.util.List<Bid>> collectBids(BidRequest request, Duration deadline);
}
