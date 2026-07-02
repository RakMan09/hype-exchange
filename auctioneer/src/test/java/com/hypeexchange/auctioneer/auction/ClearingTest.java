package com.hypeexchange.auctioneer.auction;

import com.hypeexchange.common.model.AuctionType;
import com.hypeexchange.common.model.Bid;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClearingTest {

    private static Bid bid(String bidder, long price) {
        return new Bid("req-1", bidder, price, 0L);
    }

    @Test
    void firstPriceWinnerPaysOwnBid() {
        var bids = List.of(bid("a", 500), bid("b", 300), bid("c", 700));
        var outcome = Clearing.clear(bids, 100, AuctionType.FIRST_PRICE).orElseThrow();
        assertEquals("c", outcome.winnerId());
        assertEquals(700, outcome.clearingPrice());
    }

    @Test
    void secondPriceWinnerPaysSecondHighest() {
        var bids = List.of(bid("a", 500), bid("b", 300), bid("c", 700));
        var outcome = Clearing.clear(bids, 100, AuctionType.SECOND_PRICE).orElseThrow();
        assertEquals("c", outcome.winnerId());
        assertEquals(500, outcome.clearingPrice());
    }

    @Test
    void secondPriceWithSingleEligibleBidPaysFloor() {
        var bids = List.of(bid("a", 800));
        var outcome = Clearing.clear(bids, 250, AuctionType.SECOND_PRICE).orElseThrow();
        assertEquals("a", outcome.winnerId());
        assertEquals(250, outcome.clearingPrice());
    }

    @Test
    void floorPriceExcludesLowBids() {
        var bids = List.of(bid("a", 90), bid("b", 120));
        var outcome = Clearing.clear(bids, 100, AuctionType.FIRST_PRICE).orElseThrow();
        assertEquals("b", outcome.winnerId());
        assertEquals(120, outcome.clearingPrice());
    }

    @Test
    void bidBelowFloorForSecondPriceFallsBackToFloor() {
        // Only one bid clears the floor; second price should be the floor, not the
        // sub-floor bid.
        var bids = List.of(bid("a", 90), bid("b", 500));
        var outcome = Clearing.clear(bids, 100, AuctionType.SECOND_PRICE).orElseThrow();
        assertEquals("b", outcome.winnerId());
        assertEquals(100, outcome.clearingPrice());
    }

    @Test
    void tiesBrokenDeterministicallyByBidderId() {
        var bids = List.of(bid("z", 500), bid("a", 500), bid("m", 500));
        var outcome = Clearing.clear(bids, 100, AuctionType.FIRST_PRICE).orElseThrow();
        assertEquals("a", outcome.winnerId());
    }

    @Test
    void noBidsIsUnfilled() {
        assertTrue(Clearing.clear(List.of(), 100, AuctionType.FIRST_PRICE).isEmpty());
    }

    @Test
    void allBidsBelowFloorIsUnfilled() {
        var bids = List.of(bid("a", 10), bid("b", 20));
        Optional<Clearing.Outcome> outcome = Clearing.clear(bids, 100, AuctionType.SECOND_PRICE);
        assertFalse(outcome.isPresent());
    }

    @Test
    void candidatesRankedHighestFirstWithSecondPricePerCandidate() {
        var bids = List.of(bid("a", 500), bid("b", 300), bid("c", 700));
        var ranked = Clearing.candidates(bids, 100, AuctionType.SECOND_PRICE);
        assertEquals(3, ranked.size());
        // c is top and would pay 500 (a's bid); a is next and would pay 300 (b's bid).
        assertEquals("c", ranked.get(0).winnerId());
        assertEquals(500, ranked.get(0).clearingPrice());
        assertEquals("a", ranked.get(1).winnerId());
        assertEquals(300, ranked.get(1).clearingPrice());
        assertEquals("b", ranked.get(2).winnerId());
        assertEquals(100, ranked.get(2).clearingPrice());
    }
}
