package com.hypeexchange.auctioneer.auction;

import com.hypeexchange.common.model.AuctionResult;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;

/**
 * A hot, multicast stream of auction results for live consumers (the dashboard SSE
 * endpoint). Uses a bounded buffer and drops on backpressure so slow subscribers
 * never slow the auction hot path.
 */
@Component
public class AuctionResultStream {

    private final Sinks.Many<AuctionResult> sink =
            Sinks.many().multicast().onBackpressureBuffer(1024, false);

    // Auctions complete on many threads concurrently; busy-loop briefly on the
    // rare concurrent-emit contention rather than failing the emit.
    private final Sinks.EmitFailureHandler emitHandler =
            Sinks.EmitFailureHandler.busyLooping(Duration.ofMillis(10));

    public void publish(AuctionResult result) {
        sink.emitNext(result, emitHandler);
    }

    public Flux<AuctionResult> flux() {
        return sink.asFlux();
    }
}
