package com.hypeexchange.auctioneer.auction;

import com.hypeexchange.common.model.AuctionType;
import com.hypeexchange.common.model.Bid;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Pure auction clearing logic. Given the bids that arrived before the deadline,
 * a floor price and a clearing rule, compute the winner and the price they pay.
 *
 * <p>Kept free of I/O so it can be exhaustively unit tested (first/second price,
 * ties, floors, empty input).
 */
public final class Clearing {

    private Clearing() {
    }

    /** Winner + clearing price, or empty when no bid clears the floor. */
    public record Outcome(String winnerId, long clearingPrice) {
    }

    /**
     * Determines the auction outcome (the top candidate).
     *
     * @param bids        bids collected before the deadline (may be empty)
     * @param floorPrice  reserve price; bids strictly below are ignored
     * @param type        clearing rule
     * @return the outcome, or {@link Optional#empty()} if unfilled
     */
    public static Optional<Outcome> clear(List<Bid> bids, long floorPrice, AuctionType type) {
        List<Outcome> ranked = candidates(bids, floorPrice, type);
        return ranked.isEmpty() ? Optional.empty() : Optional.of(ranked.get(0));
    }

    /**
     * Ranks every eligible bid into an ordered list of candidate outcomes, each with
     * the price that candidate would pay if it were the winner. This lets the caller
     * fall through to the next-best bidder when the top one is rejected (e.g. budget
     * exhausted) while keeping correct clearing-price semantics for each candidate.
     *
     * @return ranked candidates, highest first (empty if none clear the floor)
     */
    public static List<Outcome> candidates(List<Bid> bids, long floorPrice, AuctionType type) {
        // Deterministic ordering: highest price first, then lowest bidderId to break ties.
        List<Bid> eligible = bids.stream()
                .filter(b -> b != null && b.price() >= floorPrice)
                .sorted(Comparator.comparingLong(Bid::price).reversed()
                        .thenComparing(Bid::bidderId))
                .toList();

        return java.util.stream.IntStream.range(0, eligible.size())
                .mapToObj(i -> {
                    Bid candidate = eligible.get(i);
                    long price = switch (type) {
                        case FIRST_PRICE -> candidate.price();
                        case SECOND_PRICE -> {
                            // Vickrey: pay the next-highest eligible bid, never below the floor.
                            long next = i + 1 < eligible.size()
                                    ? eligible.get(i + 1).price() : floorPrice;
                            yield Math.max(next, floorPrice);
                        }
                    };
                    return new Outcome(candidate.bidderId(), price);
                })
                .toList();
    }
}
