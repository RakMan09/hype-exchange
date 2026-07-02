package com.hypeexchange.auctioneer.auction;

import com.hypeexchange.auctioneer.bidder.BidderGateway;
import com.hypeexchange.auctioneer.budget.BudgetService;
import com.hypeexchange.auctioneer.budget.NoopBudgetService;
import com.hypeexchange.auctioneer.config.AuctionProperties;
import com.hypeexchange.auctioneer.events.EventPublisher;
import com.hypeexchange.auctioneer.events.FunnelSimulator;
import com.hypeexchange.auctioneer.metrics.LiveMetrics;
import com.hypeexchange.common.model.AuctionResult;
import com.hypeexchange.common.model.AuctionType;
import com.hypeexchange.common.model.Bid;
import com.hypeexchange.common.model.BidRequest;
import com.hypeexchange.common.model.Click;
import com.hypeexchange.common.model.Conversion;
import com.hypeexchange.common.model.Impression;
import com.hypeexchange.common.model.Invest;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuctionServiceTest {

    private static final BidRequest REQUEST =
            new BidRequest("req-1", "meme-1", "meme", 100, 1_000L);

    private static class FakeGateway implements BidderGateway {
        private final List<Bid> bids;
        private final int count;

        FakeGateway(int count, List<Bid> bids) {
            this.count = count;
            this.bids = bids;
        }

        @Override
        public int bidderCount() {
            return count;
        }

        @Override
        public Mono<List<Bid>> collectBids(BidRequest request, Duration deadline) {
            return Mono.just(bids);
        }
    }

    private static class CapturingPublisher implements EventPublisher {
        final List<AuctionResult> results = new CopyOnWriteArrayList<>();
        final List<Impression> impressions = new CopyOnWriteArrayList<>();

        public void publishAuctionResult(AuctionResult r) {
            results.add(r);
        }

        public void publishImpression(Impression i) {
            impressions.add(i);
        }

        public void publishClick(Click c) {
        }

        public void publishInvest(Invest i) {
        }

        public void publishConversion(Conversion c) {
        }
    }

    private static AuctionProperties props(AuctionType type, boolean enforceBudgets) {
        AuctionProperties p = new AuctionProperties();
        p.setType(type);
        p.setEnforceBudgets(enforceBudgets);
        p.setDeadlineMs(100);
        return p;
    }

    private AuctionService service(BidderGateway gateway, BudgetService budget,
                                  EventPublisher publisher, AuctionProperties props) {
        LiveMetrics metrics = new LiveMetrics(budget);
        FunnelSimulator funnel = new FunnelSimulator(publisher);
        return new AuctionService(gateway, budget, publisher, funnel, metrics,
                new AuctionResultStream(), props);
    }

    @Test
    void filledFirstPriceAuctionPicksHighestBidder() {
        var gateway = new FakeGateway(3, List.of(
                new Bid("req-1", "a", 500, 0),
                new Bid("req-1", "b", 700, 0),
                new Bid("req-1", "c", 300, 0)));
        var publisher = new CapturingPublisher();
        var service = service(gateway, new NoopBudgetService(), publisher,
                props(AuctionType.FIRST_PRICE, false));

        AuctionResult result = service.runAuction(REQUEST).block();

        assertTrue(result.filled());
        assertEquals("b", result.winnerId());
        assertEquals(700, result.clearingPrice());
        assertEquals(3, result.participants());
        assertEquals(1, publisher.results.size());
        assertEquals(1, publisher.impressions.size());
        assertEquals("b", publisher.impressions.get(0).bidderId());
    }

    @Test
    void secondPriceAuctionChargesSecondHighest() {
        var gateway = new FakeGateway(2, List.of(
                new Bid("req-1", "a", 500, 0),
                new Bid("req-1", "b", 700, 0)));
        var service = service(gateway, new NoopBudgetService(), new CapturingPublisher(),
                props(AuctionType.SECOND_PRICE, false));

        AuctionResult result = service.runAuction(REQUEST).block();

        assertEquals("b", result.winnerId());
        assertEquals(500, result.clearingPrice());
    }

    @Test
    void noEligibleBidsProducesUnfilledResult() {
        var gateway = new FakeGateway(2, List.of(
                new Bid("req-1", "a", 10, 0),
                new Bid("req-1", "b", 20, 0)));
        var publisher = new CapturingPublisher();
        var service = service(gateway, new NoopBudgetService(), publisher,
                props(AuctionType.FIRST_PRICE, false));

        AuctionResult result = service.runAuction(REQUEST).block();

        assertFalse(result.filled());
        assertTrue(publisher.impressions.isEmpty(), "no impression when unfilled");
        assertEquals(1, publisher.results.size());
    }

    @Test
    void droppedBidsCountedWhenFewerBidsThanBidders() {
        var gateway = new FakeGateway(5, List.of(new Bid("req-1", "a", 500, 0)));
        var service = service(gateway, new NoopBudgetService(), new CapturingPublisher(),
                props(AuctionType.FIRST_PRICE, false));

        AuctionResult result = service.runAuction(REQUEST).block();

        assertEquals(1, result.participants());
        assertEquals(4, result.droppedBids());
    }

    @Test
    void budgetRejectionFallsThroughToNextBestBidder() {
        var gateway = new FakeGateway(3, List.of(
                new Bid("req-1", "rich", 900, 0),
                new Bid("req-1", "broke", 700, 0),
                new Bid("req-1", "ok", 500, 0)));
        // "broke" (the top-ranked candidate at 900) is rejected; next candidate wins.
        BudgetService budget = new BudgetService() {
            final Set<String> broke = Set.of("rich");

            public Mono<Boolean> tryReserve(String bidderId, String memeId, long amount) {
                return Mono.just(!broke.contains(bidderId));
            }

            public Mono<Long> remainingBudget(String bidderId) {
                return Mono.just(0L);
            }
        };
        var service = service(gateway, budget, new CapturingPublisher(),
                props(AuctionType.FIRST_PRICE, true));

        AuctionResult result = service.runAuction(REQUEST).block();

        assertTrue(result.filled());
        assertEquals("broke", result.winnerId());
        assertEquals(700, result.clearingPrice());
    }

    @Test
    void allCandidatesRejectedByBudgetProducesUnfilled() {
        var gateway = new FakeGateway(2, List.of(
                new Bid("req-1", "a", 900, 0),
                new Bid("req-1", "b", 700, 0)));
        BudgetService alwaysReject = new BudgetService() {
            public Mono<Boolean> tryReserve(String bidderId, String memeId, long amount) {
                return Mono.just(false);
            }

            public Mono<Long> remainingBudget(String bidderId) {
                return Mono.just(0L);
            }
        };
        var service = service(gateway, alwaysReject, new CapturingPublisher(),
                props(AuctionType.FIRST_PRICE, true));

        AuctionResult result = service.runAuction(REQUEST).block();

        assertFalse(result.filled());
    }
}
