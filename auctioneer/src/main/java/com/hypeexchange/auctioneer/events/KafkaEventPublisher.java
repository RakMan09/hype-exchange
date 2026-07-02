package com.hypeexchange.auctioneer.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hypeexchange.common.Topics;
import com.hypeexchange.common.model.AuctionResult;
import com.hypeexchange.common.model.Click;
import com.hypeexchange.common.model.Conversion;
import com.hypeexchange.common.model.Impression;
import com.hypeexchange.common.model.Invest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Emits the firehose to Kafka as JSON. Auction results are keyed by {@code memeId}
 * (so per-meme aggregates stay co-partitioned); funnel events are keyed by
 * {@code requestId} so a click always lands in the same partition as its impression.
 */
public class KafkaEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper) {
        this.kafka = kafka;
        this.mapper = mapper;
    }

    @Override
    public void publishAuctionResult(AuctionResult result) {
        send(Topics.AUCTION_RESULTS, result.memeId(), result);
    }

    @Override
    public void publishImpression(Impression impression) {
        send(Topics.IMPRESSIONS, impression.requestId(), impression);
    }

    @Override
    public void publishClick(Click click) {
        send(Topics.CLICKS, click.requestId(), click);
    }

    @Override
    public void publishInvest(Invest invest) {
        send(Topics.INVESTS, invest.requestId(), invest);
    }

    @Override
    public void publishConversion(Conversion conversion) {
        send(Topics.CONVERSIONS, conversion.requestId(), conversion);
    }

    private void send(String topic, String key, Object value) {
        try {
            kafka.send(topic, key, mapper.writeValueAsString(value));
        } catch (JsonProcessingException e) {
            log.warn("failed to serialize event for topic {}: {}", topic, e.getMessage());
        }
    }
}
