package com.hypeexchange.auctioneer.metrics;

import com.hypeexchange.auctioneer.budget.BudgetService;
import com.hypeexchange.common.model.AuctionResult;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * In-memory, near-real-time metrics computed on the auction hot path. Feeds the
 * dashboard SSE snapshot so the UI works even without the Kafka Streams pipeline
 * running; the streams app provides the rigorous windowed/attributed analytics.
 */
@Component
public class LiveMetrics {

    private final BudgetService budgetService;

    private final AtomicLong total = new AtomicLong();
    private final AtomicLong filled = new AtomicLong();
    private final LatencyReservoir latency = new LatencyReservoir(8192);

    private final Map<String, LongAdder> bidderSpend = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> bidderWins = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> memeWins = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> memeSpend = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> memeWinsLastSnapshot = new ConcurrentHashMap<>();

    private volatile long lastSnapshotCount = 0;
    private volatile long lastSnapshotTs = System.currentTimeMillis();

    public LiveMetrics(BudgetService budgetService) {
        this.budgetService = budgetService;
    }

    /** Record a completed auction (called on every auction, filled or not). */
    public void record(AuctionResult result) {
        total.incrementAndGet();
        latency.record(result.latencyMs());
        if (result.filled()) {
            filled.incrementAndGet();
            bidderSpend.computeIfAbsent(result.winnerId(), k -> new LongAdder())
                    .add(result.clearingPrice());
            bidderWins.computeIfAbsent(result.winnerId(), k -> new LongAdder()).increment();
            memeWins.computeIfAbsent(result.memeId(), k -> new LongAdder()).increment();
            memeSpend.computeIfAbsent(result.memeId(), k -> new LongAdder())
                    .add(result.clearingPrice());
        }
    }

    /** Build a snapshot; enriches bidder rows with remaining Redis budget. */
    public Mono<MetricsSnapshot> snapshot() {
        long now = System.currentTimeMillis();
        long totalNow = total.get();
        long filledNow = filled.get();

        long deltaCount = totalNow - lastSnapshotCount;
        long deltaMs = Math.max(1, now - lastSnapshotTs);
        double perSec = deltaCount * 1000.0 / deltaMs;
        lastSnapshotCount = totalNow;
        lastSnapshotTs = now;

        List<Map.Entry<String, LongAdder>> topBidderEntries = bidderSpend.entrySet().stream()
                .sorted(Comparator.comparingLong((Map.Entry<String, LongAdder> e) -> e.getValue().sum())
                        .reversed())
                .limit(10)
                .toList();

        List<MetricsSnapshot.MemeMovement> movers = memeWins.entrySet().stream()
                .sorted(Comparator.comparingLong((Map.Entry<String, LongAdder> e) -> e.getValue().sum())
                        .reversed())
                .limit(10)
                .map(e -> {
                    String meme = e.getKey();
                    long wins = e.getValue().sum();
                    long spend = memeSpend.getOrDefault(meme, new LongAdder()).sum();
                    long prev = memeWinsLastSnapshot.getOrDefault(meme, new LongAdder()).sum();
                    long velocity = wins - prev;
                    memeWinsLastSnapshot.computeIfAbsent(meme, k -> new LongAdder())
                            .add(wins - prev);
                    return new MetricsSnapshot.MemeMovement(meme, wins, spend, velocity);
                })
                .toList();

        double fillRate = totalNow == 0 ? 0.0 : (double) filledNow / totalNow;
        long p50 = latency.percentile(0.50);
        long p99 = latency.percentile(0.99);
        long max = latency.max();

        return Flux.fromIterable(topBidderEntries)
                .flatMap(e -> budgetService.remainingBudget(e.getKey())
                        .map(rem -> new MetricsSnapshot.BidderSpend(
                                e.getKey(), e.getValue().sum(),
                                bidderWins.getOrDefault(e.getKey(), new LongAdder()).sum(), rem)))
                .collectList()
                .map(bidders -> new MetricsSnapshot(now, totalNow, filledNow, fillRate, perSec,
                        p50, p99, max, bidders, movers));
    }
}
