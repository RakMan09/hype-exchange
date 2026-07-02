package com.hypeexchange.auctioneer.web;

import com.hypeexchange.auctioneer.auction.AuctionResultStream;
import com.hypeexchange.auctioneer.metrics.LiveMetrics;
import com.hypeexchange.auctioneer.metrics.MetricsSnapshot;
import com.hypeexchange.common.model.AuctionResult;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;

/**
 * Server-Sent Events endpoints for the live dashboard: a periodic aggregated
 * metrics snapshot and a raw stream of individual auction results.
 */
@RestController
@RequestMapping("/stream")
@CrossOrigin(originPatterns = "*")
public class DashboardController {

    private final LiveMetrics metrics;
    private final AuctionResultStream resultStream;

    public DashboardController(LiveMetrics metrics, AuctionResultStream resultStream) {
        this.metrics = metrics;
        this.resultStream = resultStream;
    }

    @GetMapping(path = "/metrics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<MetricsSnapshot> metricsStream() {
        return Flux.interval(Duration.ZERO, Duration.ofSeconds(1))
                .flatMap(tick -> metrics.snapshot());
    }

    @GetMapping(path = "/auctions", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<AuctionResult> auctionStream() {
        return resultStream.flux();
    }
}
