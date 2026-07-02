package com.hypeexchange.auctioneer.budget;

import com.hypeexchange.auctioneer.config.AuctionProperties;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Redis-backed budget pacing + frequency capping.
 *
 * <p>All logic runs inside a single Lua script so the check, decrement and cap
 * increment are one atomic step. This eliminates the read-then-write race where two
 * simultaneous wins for the same bidder could overspend: {@code DECRBY} that pushes
 * the balance negative is immediately refunded ({@code INCRBY}) and the win rejected.
 */
public class RedisBudgetService implements BudgetService {

    /**
     * KEYS[1] = budget:{bidder}, KEYS[2] = freq:{bidder}:{meme}
     * ARGV[1] = amount, ARGV[2] = freqCap, ARGV[3] = freqTtl, ARGV[4] = defaultBudget
     * returns 1 on reserve, 0 on reject.
     */
    private static final String RESERVE_LUA = """
            if redis.call('EXISTS', KEYS[1]) == 0 then
              redis.call('SET', KEYS[1], ARGV[4])
            end
            local freq = tonumber(redis.call('GET', KEYS[2]) or '0')
            if freq >= tonumber(ARGV[2]) then
              return 0
            end
            local remaining = redis.call('DECRBY', KEYS[1], ARGV[1])
            if remaining < 0 then
              redis.call('INCRBY', KEYS[1], ARGV[1])
              return 0
            end
            local newfreq = redis.call('INCR', KEYS[2])
            if newfreq == 1 then
              redis.call('EXPIRE', KEYS[2], ARGV[3])
            end
            return 1
            """;

    private final ReactiveStringRedisTemplate redis;
    private final AuctionProperties props;
    private final RedisScript<Long> reserveScript;

    public RedisBudgetService(ReactiveStringRedisTemplate redis, AuctionProperties props) {
        this.redis = redis;
        this.props = props;
        this.reserveScript = RedisScript.of(RESERVE_LUA, Long.class);
    }

    @Override
    public Mono<Boolean> tryReserve(String bidderId, String memeId, long amount) {
        List<String> keys = List.of(budgetKey(bidderId), freqKey(bidderId, memeId));
        return redis.execute(reserveScript, keys,
                        List.of(Long.toString(amount),
                                Long.toString(props.getFreqCap()),
                                Long.toString(props.getFreqCapTtlSeconds()),
                                Long.toString(props.getDefaultBudget())))
                .next()
                .map(result -> result != null && result == 1L)
                .defaultIfEmpty(false);
    }

    @Override
    public Mono<Long> remainingBudget(String bidderId) {
        return redis.opsForValue().get(budgetKey(bidderId))
                .map(Long::parseLong)
                .defaultIfEmpty(props.getDefaultBudget());
    }

    private static String budgetKey(String bidderId) {
        return "budget:" + bidderId;
    }

    private static String freqKey(String bidderId, String memeId) {
        return "freq:" + bidderId + ":" + memeId;
    }
}
