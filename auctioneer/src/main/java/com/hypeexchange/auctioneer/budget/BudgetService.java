package com.hypeexchange.auctioneer.budget;

import reactor.core.publisher.Mono;

/**
 * Guards a win against the bidder's remaining budget and per-meme frequency cap.
 * Implementations must make the check-and-decrement atomic so concurrent wins for
 * the same bidder cannot overspend.
 */
public interface BudgetService {

    /**
     * Atomically attempt to reserve {@code amount} against the bidder's budget while
     * respecting the per-meme frequency cap.
     *
     * @return {@code true} if the win is allowed (budget debited, cap incremented),
     * {@code false} if it was rejected (budget exhausted or cap reached).
     */
    Mono<Boolean> tryReserve(String bidderId, String memeId, long amount);

    /** Remaining budget for a bidder in integer cents (best effort, for metrics). */
    Mono<Long> remainingBudget(String bidderId);
}
