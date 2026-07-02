package com.hypeexchange.auctioneer.auction;

import com.hypeexchange.auctioneer.bidder.BidderGateway;
import com.hypeexchange.auctioneer.budget.BudgetService;
import com.hypeexchange.auctioneer.config.AuctionProperties;
import com.hypeexchange.auctioneer.events.EventPublisher;
import com.hypeexchange.auctioneer.events.FunnelSimulator;
import com.hypeexchange.auctioneer.metrics.LiveMetrics;
import com.hypeexchange.common.model.AuctionResult;
import com.hypeexchange.common.model.Bid;
import com.hypeexchange.common.model.BidRequest;
import com.hypeexchange.common.model.Impression;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/**
 * Orchestrates a single auction end to end:
 * <ol>
 *   <li>fan out to bidders with a hard deadline (stragglers dropped),</li>
 *   <li>clear first- or second-price,</li>
 *   <li>settle the winner against Redis budget + frequency caps (falling through to
 *       the next-best bidder on rejection),</li>
 *   <li>emit the firehose (auction result + impression + simulated funnel) and update
 *       live metrics.</li>
 * </ol>
 */
@Service
public class AuctionService {

    private final BidderGateway gateway;
    private final BudgetService budgetService;
    private final EventPublisher publisher;
    private final FunnelSimulator funnelSimulator;
    private final LiveMetrics metrics;
    private final AuctionResultStream resultStream;
    private final AuctionProperties props;

    public AuctionService(BidderGateway gateway,
                          BudgetService budgetService,
                          EventPublisher publisher,
                          FunnelSimulator funnelSimulator,
                          LiveMetrics metrics,
                          AuctionResultStream resultStream,
                          AuctionProperties props) {
        this.gateway = gateway;
        this.budgetService = budgetService;
        this.publisher = publisher;
        this.funnelSimulator = funnelSimulator;
        this.metrics = metrics;
        this.resultStream = resultStream;
        this.props = props;
    }

    public Mono<AuctionResult> runAuction(BidRequest request) {
        long startNanos = System.nanoTime();
        Duration deadline = Duration.ofMillis(props.getDeadlineMs());
        return gateway.collectBids(request, deadline)
                .flatMap(bids -> settle(request, bids, startNanos));
    }

    private Mono<AuctionResult> settle(BidRequest request, List<Bid> bids, long startNanos) {
        int participants = bids.size();
        int dropped = Math.max(0, gateway.bidderCount() - participants);

        List<Clearing.Outcome> ranked =
                Clearing.candidates(bids, request.floorPrice(), props.getType());

        if (ranked.isEmpty()) {
            return Mono.just(unfilled(request, participants, dropped, startNanos))
                    .doOnNext(this::onResult);
        }

        return selectWinner(ranked, request)
                .map(outcome -> filled(request, outcome, participants, dropped, startNanos))
                .defaultIfEmpty(unfilled(request, participants, dropped, startNanos))
                .doOnNext(result -> {
                    onResult(result);
                    if (result.filled()) {
                        emitImpressionAndFunnel(result);
                    }
                });
    }

    /**
     * Walk candidates highest-first, reserving budget for each until one succeeds.
     * {@code concatMap} keeps this strictly sequential; {@code next()} stops at the
     * first winner and cancels the rest.
     */
    private Mono<Clearing.Outcome> selectWinner(List<Clearing.Outcome> ranked, BidRequest request) {
        if (!props.isEnforceBudgets()) {
            return Mono.just(ranked.get(0));
        }
        return Flux.fromIterable(ranked)
                .concatMap(candidate -> budgetService
                        .tryReserve(candidate.winnerId(), request.memeId(), candidate.clearingPrice())
                        .filter(Boolean::booleanValue)
                        .map(ok -> candidate))
                .next();
    }

    private void onResult(AuctionResult result) {
        metrics.record(result);
        publisher.publishAuctionResult(result);
        resultStream.publish(result);
    }

    private void emitImpressionAndFunnel(AuctionResult result) {
        Impression imp = new Impression(result.requestId(), result.memeId(),
                result.winnerId(), result.clearingPrice(), result.ts());
        publisher.publishImpression(imp);
        funnelSimulator.simulate(imp);
    }

    private AuctionResult filled(BidRequest request, Clearing.Outcome outcome,
                                 int participants, int dropped, long startNanos) {
        return new AuctionResult(request.id(), request.memeId(), outcome.winnerId(),
                outcome.clearingPrice(), participants, elapsedMs(startNanos), true,
                props.getType(), dropped, request.ts());
    }

    private AuctionResult unfilled(BidRequest request, int participants, int dropped, long startNanos) {
        return AuctionResult.unfilled(request.id(), request.memeId(), participants,
                elapsedMs(startNanos), props.getType(), dropped, request.ts());
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }
}
