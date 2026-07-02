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

    @Test
    void slowBidderIsDroppedAndAuctionReturnsOnTime() {
        String fast = startBidder("fast", 0);
        String slow = startBidder("slow", 400);
        var gateway = gateway(List.of(fast, slow), 500);

        long start = System.nanoTime();
        List<Bid> bids = gateway.collectBids(
                        new BidRequest("req-1", "meme-1", "meme", 100, 0),
                        Duration.ofMillis(100))
                .block(Duration.ofSeconds(2));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        assertEquals(1, bids.size(), "only the fast bidder should be collected");
        assertEquals("fast", bids.get(0).bidderId());
        assertTrue(elapsedMs < 300,
                "auction must return near the deadline, took " + elapsedMs + "ms");
    }

    @Test
    void allFastBiddersAreCollected() {
        var gateway = gateway(List.of(
                startBidder("a", 0), startBidder("b", 0), startBidder("c", 0)), 500);

        List<Bid> bids = gateway.collectBids(
                        new BidRequest("req-1", "meme-1", "meme", 100, 0),
                        Duration.ofMillis(200))
                .block(Duration.ofSeconds(2));

        assertEquals(3, bids.size());
    }

    @Test
    void allBiddersTimingOutYieldsEmptyResultWithoutHanging() {
        var gateway = gateway(List.of(startBidder("slow1", 400), startBidder("slow2", 400)), 500);

        long start = System.nanoTime();
        List<Bid> bids = gateway.collectBids(
                        new BidRequest("req-1", "meme-1", "meme", 100, 0),
                        Duration.ofMillis(80))
                .block(Duration.ofSeconds(2));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        assertTrue(bids.isEmpty());
        assertTrue(elapsedMs < 300, "must not wait for slow bidders, took " + elapsedMs + "ms");
    }
}
