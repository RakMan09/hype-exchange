package com.hypeexchange.streams;

import com.hypeexchange.common.Topics;
import com.hypeexchange.common.model.AuctionResult;
import com.hypeexchange.common.model.AuctionType;
import com.hypeexchange.common.model.Click;
import com.hypeexchange.common.model.Impression;
import com.hypeexchange.streams.model.AttributedClick;
import com.hypeexchange.streams.model.BidderWindowMetrics;
import com.hypeexchange.streams.model.MemeAttribution;
import com.hypeexchange.streams.model.MemeWindowMetrics;
import com.hypeexchange.streams.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Deterministic replay of a known firehose through {@link AnalyticsTopology} using
 * {@link TopologyTestDriver}. Asserts window aggregates and event-time attribution
 * match hand-computed expected results, including out-of-order arrivals.
 */
class AnalyticsTopologyTest {

    private TopologyTestDriver driver;

    private TestInputTopic<String, AuctionResult> resultsIn;
    private TestInputTopic<String, Impression> impressionsIn;
    private TestInputTopic<String, Click> clicksIn;

    private TestOutputTopic<String, MemeWindowMetrics> memeOut;
    private TestOutputTopic<String, BidderWindowMetrics> bidderOut;
    private TestOutputTopic<String, AttributedClick> attributedOut;
    private TestOutputTopic<String, MemeAttribution> attributionOut;

    private final JsonSerde<AuctionResult> resultSerde = JsonSerde.of(AuctionResult.class);
    private final JsonSerde<Impression> impressionSerde = JsonSerde.of(Impression.class);
    private final JsonSerde<Click> clickSerde = JsonSerde.of(Click.class);

    @BeforeEach
    void setUp() {
        Topology topology = AnalyticsTopology.build(AnalyticsConfig.defaults());
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "analytics-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(topology, props);

        var str = Serdes.String();
        resultsIn = driver.createInputTopic(Topics.AUCTION_RESULTS,
                str.serializer(), resultSerde.serializer());
        impressionsIn = driver.createInputTopic(Topics.IMPRESSIONS,
                str.serializer(), impressionSerde.serializer());
        clicksIn = driver.createInputTopic(Topics.CLICKS,
                str.serializer(), clickSerde.serializer());

        memeOut = driver.createOutputTopic(Topics.METRICS_MEME,
                str.deserializer(), JsonSerde.of(MemeWindowMetrics.class).deserializer());
        bidderOut = driver.createOutputTopic(Topics.METRICS_BIDDER,
                str.deserializer(), JsonSerde.of(BidderWindowMetrics.class).deserializer());
        attributedOut = driver.createOutputTopic(Topics.ATTRIBUTED_CLICKS,
                str.deserializer(), JsonSerde.of(AttributedClick.class).deserializer());
        attributionOut = driver.createOutputTopic(Topics.METRICS_ATTRIBUTION,
                str.deserializer(), JsonSerde.of(MemeAttribution.class).deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private AuctionResult filled(String meme, String winner, long price, long ts) {
        return new AuctionResult("req-" + ts, meme, winner, price, 3, 5, true,
                AuctionType.SECOND_PRICE, 0, ts);
    }

    private AuctionResult unfilled(String meme, long ts) {
        return AuctionResult.unfilled("req-" + ts, meme, 2, 5, AuctionType.SECOND_PRICE, 0, ts);
    }

    /** Last emitted value per key (windowed aggregates re-emit on each update). */
    private static <V> Map<String, V> latestByKey(List<KeyValue<String, V>> records) {
        return records.stream().collect(Collectors.toMap(
                kv -> kv.key, kv -> kv.value, (a, b) -> b));
    }

    @Test
    void memeMetricsAggregateWithinWindow() {
        // All within window [0, 10000).
        resultsIn.pipeInput("meme-1", filled("meme-1", "a", 500, 1_000));
        resultsIn.pipeInput("meme-1", filled("meme-1", "b", 300, 2_000));
        resultsIn.pipeInput("meme-1", unfilled("meme-1", 3_000));

        Map<String, MemeWindowMetrics> latest = latestByKey(memeOut.readKeyValuesToList());
        MemeWindowMetrics m = latest.get("meme-1");

        assertThat(m).isNotNull();
        assertThat(m.auctions()).isEqualTo(3);
        assertThat(m.fills()).isEqualTo(2);
        assertThat(m.spend()).isEqualTo(800);
        assertThat(m.avgClearingPrice()).isEqualTo(400);
        assertThat(m.fillRate()).isEqualTo(2.0 / 3.0, org.assertj.core.data.Offset.offset(1e-9));
    }

    @Test
    void bidderMetricsTrackSpendPerWinner() {
        resultsIn.pipeInput("meme-1", filled("meme-1", "a", 500, 1_000));
        resultsIn.pipeInput("meme-1", filled("meme-1", "a", 200, 2_000));
        resultsIn.pipeInput("meme-1", filled("meme-1", "b", 300, 3_000));

        Map<String, BidderWindowMetrics> latest = latestByKey(bidderOut.readKeyValuesToList());

        assertThat(latest.get("a").wins()).isEqualTo(2);
        assertThat(latest.get("a").spend()).isEqualTo(700);
        assertThat(latest.get("a").avgClearingPrice()).isEqualTo(350);
        assertThat(latest.get("b").wins()).isEqualTo(1);
        assertThat(latest.get("b").spend()).isEqualTo(300);
    }

    @Test
    void clicksAreAttributedToImpressionsEvenWhenOutOfOrder() {
        // Impression for req-1, then its click.
        impressionsIn.pipeInput("req-1", new Impression("req-1", "meme-1", "a", 500, 1_000));
        clicksIn.pipeInput("req-1", new Click("req-1", "meme-1", "a", 1_500));

        // Out of order: the click for req-2 arrives BEFORE its impression.
        clicksIn.pipeInput("req-2", new Click("req-2", "meme-1", "a", 2_500));
        impressionsIn.pipeInput("req-2", new Impression("req-2", "meme-1", "a", 400, 2_000));

        List<KeyValue<String, AttributedClick>> attributed = attributedOut.readKeyValuesToList();

        assertThat(attributed).hasSize(2);
        Map<String, AttributedClick> byReq = attributed.stream()
                .collect(Collectors.toMap(kv -> kv.value.requestId(), kv -> kv.value));
        assertThat(byReq.get("req-1").impressionTs()).isEqualTo(1_000);
        assertThat(byReq.get("req-1").clickTs()).isEqualTo(1_500);
        assertThat(byReq.get("req-1").latencyMs()).isEqualTo(500);
        assertThat(byReq.get("req-2").impressionTs()).isEqualTo(2_000);
        assertThat(byReq.get("req-2").clickTs()).isEqualTo(2_500);
    }

    @Test
    void attributionMetricsCountClicksInImpressionWindow() {
        // Two impressions and two attributed clicks, all belonging to window [0,10000).
        impressionsIn.pipeInput("req-1", new Impression("req-1", "meme-1", "a", 500, 1_000));
        impressionsIn.pipeInput("req-2", new Impression("req-2", "meme-1", "a", 400, 2_000));
        // Impression with no click -> lowers CTR below 1.0.
        impressionsIn.pipeInput("req-3", new Impression("req-3", "meme-1", "a", 400, 3_000));
        clicksIn.pipeInput("req-1", new Click("req-1", "meme-1", "a", 1_500));
        clicksIn.pipeInput("req-2", new Click("req-2", "meme-1", "a", 2_500));

        Map<String, MemeAttribution> latest = latestByKey(attributionOut.readKeyValuesToList());
        MemeAttribution a = latest.get("meme-1");

        assertThat(a).isNotNull();
        assertThat(a.impressions()).isEqualTo(3);
        assertThat(a.attributedClicks()).isEqualTo(2);
        assertThat(a.ctr()).isEqualTo(2.0 / 3.0, org.assertj.core.data.Offset.offset(1e-9));
    }

    @Test
    void eventsInDifferentWindowsAggregateSeparately() {
        // One auction in window [0,10000), one in [10000,20000).
        resultsIn.pipeInput("meme-1", filled("meme-1", "a", 500, 1_000));
        resultsIn.pipeInput("meme-1", filled("meme-1", "a", 900, 12_000));

        List<KeyValue<String, MemeWindowMetrics>> all = memeOut.readKeyValuesToList();
        // Two distinct windows should each report a single auction.
        List<MemeWindowMetrics> distinctWindows = all.stream()
                .map(kv -> kv.value)
                .filter(m -> m.auctions() == 1)
                .collect(Collectors.toList());
        assertThat(distinctWindows).extracting(MemeWindowMetrics::windowStart)
                .contains(0L, 10_000L);
    }
}
