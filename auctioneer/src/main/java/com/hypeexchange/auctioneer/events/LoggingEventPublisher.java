package com.hypeexchange.auctioneer.events;

import com.hypeexchange.common.model.AuctionResult;
import com.hypeexchange.common.model.Click;
import com.hypeexchange.common.model.Conversion;
import com.hypeexchange.common.model.Impression;
import com.hypeexchange.common.model.Invest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** No-Kafka fallback that just logs the firehose (used when Kafka is disabled). */
public class LoggingEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(LoggingEventPublisher.class);

    @Override
    public void publishAuctionResult(AuctionResult result) {
        log.debug("auction-result {}", result);
    }

    @Override
    public void publishImpression(Impression impression) {
        log.debug("impression {}", impression);
    }

    @Override
    public void publishClick(Click click) {
        log.debug("click {}", click);
    }

    @Override
    public void publishInvest(Invest invest) {
        log.debug("invest {}", invest);
    }

    @Override
    public void publishConversion(Conversion conversion) {
        log.debug("conversion {}", conversion);
    }
}
