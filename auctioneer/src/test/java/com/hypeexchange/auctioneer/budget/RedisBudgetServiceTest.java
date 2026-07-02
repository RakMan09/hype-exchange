package com.hypeexchange.auctioneer.budget;

import com.hypeexchange.auctioneer.config.AuctionProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Flux;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Concurrency tests for {@link RedisBudgetService} against an embedded Redis (no
 * Docker needed). Proves that the atomic Lua reserve never overspends and that the
 * frequency cap holds under a burst of simultaneous wins.
 */
class RedisBudgetServiceTest {

    private RedisServer redisServer;
    private LettuceConnectionFactory connectionFactory;
    private ReactiveStringRedisTemplate template;

    @BeforeEach
    void setUp() throws IOException {
        int port = freePort();
        redisServer = new RedisServer(port);
        redisServer.start();
        connectionFactory = new LettuceConnectionFactory("localhost", port);
        connectionFactory.afterPropertiesSet();
        connectionFactory.start();
        template = new ReactiveStringRedisTemplate(connectionFactory);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
        if (redisServer != null) {
            redisServer.stop();
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private RedisBudgetService service(long budget, long freqCap) {
        AuctionProperties props = new AuctionProperties();
        props.setDefaultBudget(budget);
        props.setFreqCap(freqCap);
        props.setFreqCapTtlSeconds(60);
        return new RedisBudgetService(template, props);
    }

    @Test
    void concurrentWinsNeverOverspendTheBudget() {
        // Budget affords exactly 10 wins of 100 cents. Fire 50 concurrent reserves
        // against distinct memes (so the frequency cap never interferes).
        var service = service(1_000L, 1_000_000L);

        List<Boolean> outcomes = Flux.range(0, 50)
                .flatMap(i -> service.tryReserve("bidder-1", "meme-" + i, 100L), 50)
                .collectList()
                .block();

        long successes = outcomes.stream().filter(Boolean::booleanValue).count();
        assertEquals(10, successes, "budget must permit exactly 10 wins");

        Long remaining = service.remainingBudget("bidder-1").block();
        assertEquals(0L, remaining, "budget must land at exactly zero, never negative");
        assertTrue(remaining >= 0, "budget must never go negative");
    }

    @Test
    void frequencyCapLimitsWinsPerMeme() {
        // Budget is effectively unlimited; only the per-meme cap of 3 should bind.
        var service = service(10_000_000L, 3L);

        List<Boolean> outcomes = Flux.range(0, 20)
                .flatMap(i -> service.tryReserve("bidder-1", "meme-hot", 100L), 20)
                .collectList()
                .block();

        long successes = outcomes.stream().filter(Boolean::booleanValue).count();
        assertEquals(3, successes, "frequency cap must permit exactly 3 wins per meme");
    }

    @Test
    void differentMemesHaveIndependentCaps() {
        var service = service(10_000_000L, 2L);

        long memeA = countSuccess(Flux.range(0, 5)
                .flatMap(i -> service.tryReserve("bidder-1", "meme-a", 100L), 5)
                .collectList().block());
        long memeB = countSuccess(Flux.range(0, 5)
                .flatMap(i -> service.tryReserve("bidder-1", "meme-b", 100L), 5)
                .collectList().block());

        assertEquals(2, memeA);
        assertEquals(2, memeB);
    }

    private static long countSuccess(List<Boolean> outcomes) {
        return outcomes.stream().filter(Boolean::booleanValue).count();
    }
}
