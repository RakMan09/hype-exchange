package com.hypeexchange.auctioneer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hypeexchange.auctioneer.budget.BudgetService;
import com.hypeexchange.auctioneer.budget.NoopBudgetService;
import com.hypeexchange.auctioneer.budget.RedisBudgetService;
import com.hypeexchange.auctioneer.events.EventPublisher;
import com.hypeexchange.auctioneer.events.KafkaEventPublisher;
import com.hypeexchange.auctioneer.events.LoggingEventPublisher;
import io.netty.channel.ChannelOption;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.reactive.function.client.WebClientCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.kafka.core.KafkaTemplate;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

@Configuration
@EnableConfigurationProperties(AuctionProperties.class)
public class AuctioneerConfig {

    /**
     * Latency hardening: a large, non-blocking connection pool so many concurrent
     * auctions can fan out in parallel without queueing on connection acquisition,
     * plus a tight connect timeout so an unreachable bidder fails fast rather than
     * eating into the auction deadline.
     */
    @Bean
    public WebClientCustomizer bidderWebClientCustomizer() {
        ConnectionProvider provider = ConnectionProvider.builder("bidders")
                .maxConnections(4000)
                .pendingAcquireMaxCount(-1)
                .build();
        HttpClient httpClient = HttpClient.create(provider)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 200);
        return builder -> builder.clientConnector(new ReactorClientHttpConnector(httpClient));
    }

    @Bean
    @ConditionalOnProperty(prefix = "hype.auction", name = "enforce-budgets",
            havingValue = "true", matchIfMissing = true)
    public BudgetService redisBudgetService(ReactiveStringRedisTemplate template,
                                            AuctionProperties props) {
        return new RedisBudgetService(template, props);
    }

    @Bean
    @ConditionalOnProperty(prefix = "hype.auction", name = "enforce-budgets", havingValue = "false")
    public BudgetService noopBudgetService() {
        return new NoopBudgetService();
    }

    @Bean
    @ConditionalOnProperty(prefix = "hype.kafka", name = "enabled",
            havingValue = "true", matchIfMissing = true)
    public EventPublisher kafkaEventPublisher(KafkaTemplate<String, String> kafka,
                                              ObjectMapper mapper) {
        return new KafkaEventPublisher(kafka, mapper);
    }

    @Bean
    @ConditionalOnProperty(prefix = "hype.kafka", name = "enabled", havingValue = "false")
    public EventPublisher loggingEventPublisher() {
        return new LoggingEventPublisher();
    }
}
