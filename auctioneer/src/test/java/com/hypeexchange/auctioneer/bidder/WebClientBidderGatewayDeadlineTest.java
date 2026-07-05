package com.hypeexchange.auctioneer.bidder;

import com.hypeexchange.auctioneer.config.AuctionProperties;
import com.hypeexchange.common.model.Bid;
import com.hypeexchange.common.model.BidRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the hard-deadline semantics against real bidder HTTP endpoints: a bidder
 * that responds after the deadline must be dropped, and the auction must still return
 * promptly with whatever arrived in time.
 */
class WebClientBidderGatewayDeadlineTest {

    private final List<DisposableServer> servers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        servers.forEach(DisposableServer::disposeNow);
        servers.clear();
    }

    private String startBidder(String bidderId, long delayMs) {
        String json = "{\"requestId\":\"req-1\",\"bidderId\":\"" + bidderId
                + "\",\"price\":500,\"ts\":0}";
        DisposableServer server = HttpServer.create()
                .port(0)
                .route(routes -> routes.post("/bid", (req, res) -> {
                    Mono<String> body = Mono.just(json);
                    if (delayMs > 0) {
                        body = body.delayElement(Duration.ofMillis(delayMs));
                    }
                    return res.header(HttpHeaderNames.CONTENT_TYPE, "application/json")
                            .sendString(body);
                }))
                .bindNow();
        servers.add(server);
        return "http://localhost:" + server.port();
    }

    private WebClientBidderGateway gateway(List<String> urls, long timeoutMs) {
        AuctionProperties props = new AuctionProperties();
        props.setBidders(urls);
        props.setBidderTimeoutMs(timeoutMs);
        return new WebClientBidderGateway(props, WebClient.builder());
    }

    private static final BidRequest REQ = new BidRequest("req-1", "meme-1", "meme", 100, 0);

    /**
     * Prime connections, Netty and the JIT with one generous-deadline call so the
     * subsequent timed assertions are not skewed by cold-start latency (important on
     * slow/shared CI runners).
     */
    private static void warmUp(WebClientBidderGateway gateway) {
        gateway.collectBids(REQ, Duration.ofSeconds(5)).block(Duration.ofSeconds(10));
    }

    @Test
    void slowBidderIsDroppedAndAuctionReturnsOnTime() {
        // Slow bidder is far beyond the deadline; the deadline must exclude it.
        String fast = startBidder("fast", 0);
        String slow = startBidder("slow", 1_000);
        var gateway = gateway(List.of(fast, slow), 2_000);
        warmUp(gateway);

        long start = System.nanoTime();
        List<Bid> bids = gateway.collectBids(REQ, Duration.ofMillis(300))
                .block(Duration.ofSeconds(5));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        assertEquals(1, bids.size(), "only the fast bidder should be collected");
        assertEquals("fast", bids.get(0).bidderId());
        assertTrue(elapsedMs < 800,
                "auction must not wait for the 1000ms bidder, took " + elapsedMs + "ms");
    }

    @Test
    void allFastBiddersAreCollected() {
        var gateway = gateway(List.of(
                startBidder("a", 0), startBidder("b", 0), startBidder("c", 0)), 2_000);
        warmUp(gateway);

        // Generous deadline: this asserts completeness of collection, not tightness.
        List<Bid> bids = gateway.collectBids(REQ, Duration.ofSeconds(2))
                .block(Duration.ofSeconds(5));

        assertEquals(3, bids.size());
    }

    @Test
    void allBiddersTimingOutYieldsEmptyResultWithoutHanging() {
        var gateway = gateway(List.of(startBidder("slow1", 1_000), startBidder("slow2", 1_000)), 2_000);
        warmUp(gateway);

        long start = System.nanoTime();
        List<Bid> bids = gateway.collectBids(REQ, Duration.ofMillis(200))
                .block(Duration.ofSeconds(5));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        assertTrue(bids.isEmpty());
        assertTrue(elapsedMs < 800, "must not wait for slow bidders, took " + elapsedMs + "ms");
    }
}
