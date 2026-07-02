package com.hypeexchange.streams.model;

import com.hypeexchange.common.model.EventTimed;

/**
 * A click joined to the impression it belongs to (by {@code requestId}), within the
 * event-time attribution window. Emitted to {@code attributed-clicks}.
 *
 * <p>Its event-time is the <em>impression</em> timestamp so that downstream windowed
 * counts fold the click into the impression's window, not the window the click
 * happened to arrive in.
 *
 * @param requestId    attribution key
 * @param memeId       the meme
 * @param bidderId     the winning bidder
 * @param impressionTs impression event-time, epoch millis
 * @param clickTs      click event-time, epoch millis
 * @param latencyMs    clickTs - impressionTs
 */
public record AttributedClick(
        String requestId,
        String memeId,
        String bidderId,
        long impressionTs,
        long clickTs,
        long latencyMs) implements EventTimed {

    @Override
    public long ts() {
        return impressionTs;
    }
}
