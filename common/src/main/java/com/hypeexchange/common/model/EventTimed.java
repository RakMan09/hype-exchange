package com.hypeexchange.common.model;

/** Anything carrying an event-time timestamp (epoch millis), used for windowing. */
public interface EventTimed {
    long ts();
}
