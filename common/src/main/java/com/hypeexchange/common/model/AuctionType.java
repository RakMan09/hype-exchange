package com.hypeexchange.common.model;

/** Clearing rule used by the auctioneer. */
public enum AuctionType {
    /** Winner pays their own bid. */
    FIRST_PRICE,
    /** Winner pays the second-highest bid (Vickrey), never below the floor. */
    SECOND_PRICE
}
