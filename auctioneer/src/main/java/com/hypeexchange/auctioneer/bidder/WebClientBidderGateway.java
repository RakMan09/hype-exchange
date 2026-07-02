package com.hypeexchange.auctioneer.bidder;

import com.hypeexchange.auctioneer.config.AuctionProperties;
import com.hypeexchange.common.model.Bid;
import com.hypeexchange.common.model.BidRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/**
 * Reactive fan-out to bidder bots over HTTP.
 *
 * <p>Deadline strategy: every bidder is called in parallel; each call is wrapped
 * so any error or slowness collapses to {@link Mono#empty()}. The merged stream is
 * then cut with {@link Flux#take(Duration)}, which completes at the deadline and
 * cancels in-flight calls. Whatever arrived is returned; nothing blocks.
 */
@Component
public class WebClientBidderGateway implements BidderGateway {

    private static final Logger log = LoggerFactory.getLogger(WebClientBidderGateway.class);

    private final List<WebClient> bidderClients;
    private final Duration perCallTimeout;

    public WebClientBidderGateway(AuctionProperties props, WebClient.Builder builder) {
        this.bidderClients = props.getBidders().stream()
                .map(base -> builder.clone().baseUrl(base).build())
                .toList();
        this.perCallTimeout = Duration.ofMillis(props.getBidderTimeoutMs());
    }

    @Override
    public int bidderCount() {
        return bidderClients.size();
    }

    @Override
    public Mono<List<Bid>> collectBids(BidRequest request, Duration deadline) {
        if (bidderClients.isEmpty()) {
            return Mono.just(List.of());
        }

        List<Mono<Bid>> calls = bidderClients.stream()
                .map(client -> callBidder(client, request))
                .toList();

        return Flux.merge(calls)
                .take(deadline)              // hard cut at the aggregate deadline
                .collectList();
    }

    private Mono<Bid> callBidder(WebClient client, BidRequest request) {
        return client.post()
                .uri("/bid")
                .bodyValue(request)
                .retrieve()
                .bodyToMono(Bid.class)
                .timeout(perCallTimeout)
                .onErrorResume(ex -> {
                    if (log.isTraceEnabled()) {
                        log.trace("bidder call failed for {}: {}", request.id(), ex.toString());
                    }
                    return Mono.empty();     // slow / failing bidder simply drops out
                });
    }
}
