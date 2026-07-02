package com.hypeexchange.common;

/** Kafka topic names shared across the auctioneer (producer) and streams (consumer). */
public final class Topics {

    private Topics() {
    }

    /** Auction results (win/loss) keyed by memeId. */
    public static final String AUCTION_RESULTS = "auction-results";

    /** Impressions served, keyed by requestId. */
    public static final String IMPRESSIONS = "impressions";

    /** Clicks, keyed by requestId. Often late / out of order. */
    public static final String CLICKS = "clicks";

    /** Invest actions, keyed by requestId. */
    public static final String INVESTS = "invests";

    /** Conversions, keyed by requestId. */
    public static final String CONVERSIONS = "conversions";

    /** Output: per-meme windowed metrics produced by the streams app. */
    public static final String METRICS_MEME = "metrics-meme";

    /** Output: per-bidder windowed metrics produced by the streams app. */
    public static final String METRICS_BIDDER = "metrics-bidder";

    /** Output: impressions joined to their clicks (event-time attribution). */
    public static final String ATTRIBUTED_CLICKS = "attributed-clicks";

    /** Output: per-meme windowed impression/click/CTR attribution metrics. */
    public static final String METRICS_ATTRIBUTION = "metrics-attribution";
}
