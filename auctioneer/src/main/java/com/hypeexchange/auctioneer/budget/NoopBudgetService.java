package com.hypeexchange.auctioneer.budget;

import reactor.core.publisher.Mono;

/**
 * Budget service used when enforcement is disabled (pure M1 behaviour): every win
 * is allowed and budgets are reported as effectively unlimited.
 */
public class NoopBudgetService implements BudgetService {

    @Override
    public Mono<Boolean> tryReserve(String bidderId, String memeId, long amount) {
        return Mono.just(true);
    }

    @Override
    public Mono<Long> remainingBudget(String bidderId) {
        return Mono.just(Long.MAX_VALUE);
    }
}
