package com.hypeexchange.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/** Runs the HypeExchange analytics topology against a real Kafka cluster. */
public final class StreamsApp {

    private static final Logger log = LoggerFactory.getLogger(StreamsApp.class);

    public static void main(String[] args) {
        String bootstrap = getenv("KAFKA_BOOTSTRAP", "localhost:9092");
        String appId = getenv("STREAMS_APP_ID", "hypeexchange-streams");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,
                Integer.parseInt(getenv("STREAMS_THREADS", "2")));

        Topology topology = AnalyticsTopology.build(AnalyticsConfig.defaults());
        log.info("starting streams app '{}' against {}", appId, bootstrap);
        log.debug("topology:\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);

        streams.setUncaughtExceptionHandler(ex -> {
            log.error("uncaught streams exception", ex);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                    .StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down streams app");
            streams.close();
            latch.countDown();
        }));

        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String getenv(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? fallback : v;
    }

    private StreamsApp() {
    }
}
