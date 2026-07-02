package com.hypeexchange.streams;

import java.time.Duration;

/**
 * Windowing parameters for the analytics topology. Centralized so the running app
 * and the deterministic {@code TopologyTestDriver} tests share identical settings.
 *
 * @param windowSize        tumbling window size for meme/bidder aggregates
 * @param grace             how long after a window closes a late event is still folded in
 * @param attributionAfter  max delay between an impression and a click for the click
 *                          to be attributed to that impression
 */
public record AnalyticsConfig(Duration windowSize, Duration grace, Duration attributionAfter) {

    public static AnalyticsConfig defaults() {
        return new AnalyticsConfig(
                Duration.ofSeconds(10),
                Duration.ofSeconds(5),
                Duration.ofSeconds(30));
    }
}
