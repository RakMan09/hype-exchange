package com.hypeexchange.streams;

import com.hypeexchange.common.Topics;
import com.hypeexchange.common.model.AuctionResult;
import com.hypeexchange.common.model.Click;
import com.hypeexchange.common.model.Impression;
import com.hypeexchange.streams.agg.AttribAgg;
import com.hypeexchange.streams.agg.AttribDelta;
import com.hypeexchange.streams.agg.BidderAgg;
import com.hypeexchange.streams.agg.MemeAgg;
import com.hypeexchange.streams.model.AttributedClick;
import com.hypeexchange.streams.model.BidderWindowMetrics;
import com.hypeexchange.streams.model.MemeAttribution;
import com.hypeexchange.streams.model.MemeWindowMetrics;
import com.hypeexchange.streams.serde.EventTimeExtractor;
import com.hypeexchange.streams.serde.JsonSerde;
import com.hypeexchange.streams.serde.TimestampReassigner;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;

/**
 * The HypeExchange streaming analytics topology (M4).
 *
 * <p>Everything is event-time based via {@link EventTimeExtractor} and tumbling
 * windows carry a grace period so out-of-order / late events are still folded into
 * the window they belong to (updates are re-emitted). Three outputs are produced:
 * <ul>
 *   <li>{@code metrics-meme} — auctions, fills, fill rate, spend, eCPM per meme,</li>
 *   <li>{@code metrics-bidder} — wins, spend, eCPM per bidder,</li>
 *   <li>{@code metrics-attribution} — impressions, attributed clicks, CTR per meme,</li>
 * </ul>
 * plus {@code attributed-clicks}: each click joined to the impression it belongs to.
 *
 * <p>The class is pure topology construction (no broker), so it is exercised
 * deterministically with {@code TopologyTestDriver}.
 */
public final class AnalyticsTopology {

    private AnalyticsTopology() {
    }

    public static Topology build(AnalyticsConfig cfg) {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> str = Serdes.String();
        JsonSerde<AuctionResult> resultSerde = JsonSerde.of(AuctionResult.class);
        JsonSerde<Impression> impressionSerde = JsonSerde.of(Impression.class);
        JsonSerde<Click> clickSerde = JsonSerde.of(Click.class);
        JsonSerde<AttributedClick> attributedSerde = JsonSerde.of(AttributedClick.class);
        JsonSerde<MemeAgg> memeAggSerde = JsonSerde.of(MemeAgg.class);
        JsonSerde<BidderAgg> bidderAggSerde = JsonSerde.of(BidderAgg.class);
        JsonSerde<AttribDelta> attribDeltaSerde = JsonSerde.of(AttribDelta.class);
        JsonSerde<AttribAgg> attribAggSerde = JsonSerde.of(AttribAgg.class);
        JsonSerde<MemeWindowMetrics> memeMetricsSerde = JsonSerde.of(MemeWindowMetrics.class);
        JsonSerde<BidderWindowMetrics> bidderMetricsSerde = JsonSerde.of(BidderWindowMetrics.class);
        JsonSerde<MemeAttribution> attribMetricsSerde = JsonSerde.of(MemeAttribution.class);

        Consumed<String, AuctionResult> resultsConsumed = Consumed
                .with(str, resultSerde)
                .withTimestampExtractor(new EventTimeExtractor());
        Consumed<String, Impression> impressionsConsumed = Consumed
                .with(str, impressionSerde)
                .withTimestampExtractor(new EventTimeExtractor());
        Consumed<String, Click> clicksConsumed = Consumed
                .with(str, clickSerde)
                .withTimestampExtractor(new EventTimeExtractor());

        TimeWindows windows = TimeWindows.ofSizeAndGrace(cfg.windowSize(), cfg.grace());

        // Auction results arrive keyed by memeId.
        KStream<String, AuctionResult> results = builder.stream(Topics.AUCTION_RESULTS, resultsConsumed);

        // ---- metrics-meme: per-meme windowed auction metrics ----
        results
                .groupByKey(Grouped.with(str, resultSerde))
                .windowedBy(windows)
                .aggregate(MemeAgg::empty, (k, v, agg) -> agg.add(v),
                        Materialized.with(str, memeAggSerde))
                .toStream()
                .map((wk, agg) -> KeyValue.pair(wk.key(), new MemeWindowMetrics(
                        wk.key(), wk.window().start(), wk.window().end(),
                        agg.auctions(), agg.fills(), agg.spend(),
                        agg.avgClearingPrice(), agg.fillRate())))
                .to(Topics.METRICS_MEME, Produced.with(str, memeMetricsSerde));

        // ---- metrics-bidder: per-bidder windowed spend metrics ----
        results
                .filter((k, v) -> v != null && v.filled() && v.winnerId() != null)
                .groupBy((k, v) -> v.winnerId(), Grouped.with(str, resultSerde))
                .windowedBy(windows)
                .aggregate(BidderAgg::empty, (k, v, agg) -> agg.add(v),
                        Materialized.with(str, bidderAggSerde))
                .toStream()
                .map((wk, agg) -> KeyValue.pair(wk.key(), new BidderWindowMetrics(
                        wk.key(), wk.window().start(), wk.window().end(),
                        agg.wins(), agg.spend(), agg.avgClearingPrice())))
                .to(Topics.METRICS_BIDDER, Produced.with(str, bidderMetricsSerde));

        // ---- attribution: join clicks to their impression by requestId ----
        KStream<String, Impression> impressions = builder.stream(Topics.IMPRESSIONS, impressionsConsumed);
        KStream<String, Click> clicks = builder.stream(Topics.CLICKS, clicksConsumed);

        // Symmetric event-time join window: a click is attributed to an impression with
        // the same requestId within +/- attributionAfter, tolerating late arrivals via grace.
        JoinWindows joinWindows = JoinWindows
                .ofTimeDifferenceAndGrace(cfg.attributionAfter(), cfg.grace());

        KStream<String, AttributedClick> attributed = impressions.join(
                clicks,
                (imp, click) -> new AttributedClick(
                        imp.requestId(), imp.memeId(), imp.bidderId(),
                        imp.ts(), click.ts(), click.ts() - imp.ts()),
                joinWindows,
                StreamJoined.with(str, impressionSerde, clickSerde));

        attributed.to(Topics.ATTRIBUTED_CLICKS, Produced.with(str, attributedSerde));

        // ---- metrics-attribution: per-meme windowed impressions / attributed clicks / CTR ----
        // Impressions counted at their own event-time; attributed clicks re-stamped to the
        // impression's event-time so they land in the impression's window, not their own.
        KStream<String, AttribDelta> impressionDeltas = impressions
                .map((k, imp) -> KeyValue.pair(imp.memeId(), AttribDelta.impression()));

        KStream<String, AttribDelta> clickDeltas = attributed
                .map((k, ac) -> KeyValue.pair(ac.memeId(), ac))
                .process(new TimestampReassigner<>(AttributedClick::impressionTs))
                .mapValues(ac -> AttribDelta.click());

        impressionDeltas.merge(clickDeltas)
                .groupByKey(Grouped.with(str, attribDeltaSerde))
                .windowedBy(windows)
                .aggregate(AttribAgg::empty, (k, v, agg) -> agg.add(v),
                        Materialized.with(str, attribAggSerde))
                .toStream()
                .map((wk, agg) -> KeyValue.pair(wk.key(), new MemeAttribution(
                        wk.key(), wk.window().start(), wk.window().end(),
                        agg.impressions(), agg.clicks(), agg.ctr())))
                .to(Topics.METRICS_ATTRIBUTION, Produced.with(str, attribMetricsSerde));

        return builder.build();
    }
}
