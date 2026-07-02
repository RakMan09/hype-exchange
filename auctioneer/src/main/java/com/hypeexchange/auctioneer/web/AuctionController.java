package com.hypeexchange.auctioneer.web;

import com.hypeexchange.auctioneer.auction.AuctionService;
import com.hypeexchange.common.model.AuctionResult;
import com.hypeexchange.common.model.BidRequest;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.UUID;

/** Entry point for hype events: run one auction and return its result. */
@RestController
@RequestMapping("/auctions")
public class AuctionController {

    private final AuctionService auctionService;

    public AuctionController(AuctionService auctionService) {
        this.auctionService = auctionService;
    }

    @PostMapping
    public Mono<AuctionResult> auction(@RequestBody BidRequest request) {
        return auctionService.runAuction(normalize(request));
    }

    /** Fill in id/ts if the caller omitted them, so the generator can stay minimal. */
    private static BidRequest normalize(BidRequest r) {
        String id = (r.id() == null || r.id().isBlank()) ? UUID.randomUUID().toString() : r.id();
        long ts = r.ts() == 0 ? System.currentTimeMillis() : r.ts();
        return new BidRequest(id, r.memeId(), r.category(), r.floorPrice(), ts);
    }
}
