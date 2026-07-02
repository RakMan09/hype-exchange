package com.hypeexchange.auctioneer.config;

import com.hypeexchange.common.model.AuctionType;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Tunable auction behaviour, bound from the {@code hype.auction} config prefix.
 */
@ConfigurationProperties(prefix = "hype.auction")
public class AuctionProperties {

    /** Hard deadline for the fan-out/fan-in, in milliseconds. Late bids are dropped. */
    private long deadlineMs = 100;

    /** Clearing rule. */
    private AuctionType type = AuctionType.SECOND_PRICE;

    /** Base URLs of the bidder bots to fan out to. */
    private List<String> bidders = List.of();

    /** Per-request per-bidder call timeout; a slow bidder never stalls the auction. */
    private long bidderTimeoutMs = 100;

    /** Starting budget (integer cents) seeded for each bidder on first use. */
    private long defaultBudget = 10_000_000L;

    /** Max wins per bidder per meme before the frequency cap kicks in. */
    private long freqCap = 5;

    /** TTL for a frequency-cap window, in seconds. */
    private long freqCapTtlSeconds = 60;

    /** Whether to enforce Redis budgets / frequency caps (disabled => pure M1 behaviour). */
    private boolean enforceBudgets = true;

    public long getDeadlineMs() {
        return deadlineMs;
    }

    public void setDeadlineMs(long deadlineMs) {
        this.deadlineMs = deadlineMs;
    }

    public AuctionType getType() {
        return type;
    }

    public void setType(AuctionType type) {
        this.type = type;
    }

    public List<String> getBidders() {
        return bidders;
    }

    public void setBidders(List<String> bidders) {
        this.bidders = bidders;
    }

    public long getBidderTimeoutMs() {
        return bidderTimeoutMs;
    }

    public void setBidderTimeoutMs(long bidderTimeoutMs) {
        this.bidderTimeoutMs = bidderTimeoutMs;
    }

    public long getDefaultBudget() {
        return defaultBudget;
    }

    public void setDefaultBudget(long defaultBudget) {
        this.defaultBudget = defaultBudget;
    }

    public long getFreqCap() {
        return freqCap;
    }

    public void setFreqCap(long freqCap) {
        this.freqCap = freqCap;
    }

    public long getFreqCapTtlSeconds() {
        return freqCapTtlSeconds;
    }

    public void setFreqCapTtlSeconds(long freqCapTtlSeconds) {
        this.freqCapTtlSeconds = freqCapTtlSeconds;
    }

    public boolean isEnforceBudgets() {
        return enforceBudgets;
    }

    public void setEnforceBudgets(boolean enforceBudgets) {
        this.enforceBudgets = enforceBudgets;
    }
}
