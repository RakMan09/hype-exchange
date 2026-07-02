package com.hypeexchange.auctioneer.events;

import com.hypeexchange.common.model.AuctionResult;
import com.hypeexchange.common.model.Click;
import com.hypeexchange.common.model.Conversion;
import com.hypeexchange.common.model.Impression;
import com.hypeexchange.common.model.Invest;

/** Publishes the event firehose (auction results + funnel events) downstream. */
public interface EventPublisher {

    void publishAuctionResult(AuctionResult result);

    void publishImpression(Impression impression);

    void publishClick(Click click);

    void publishInvest(Invest invest);

    void publishConversion(Conversion conversion);
}
