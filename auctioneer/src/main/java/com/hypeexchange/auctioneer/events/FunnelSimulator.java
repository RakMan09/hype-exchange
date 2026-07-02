package com.hypeexchange.auctioneer.events;

import com.hypeexchange.common.model.Click;
import com.hypeexchange.common.model.Conversion;
import com.hypeexchange.common.model.Impression;
import com.hypeexchange.common.model.Invest;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Simulates the downstream funnel for a served impression: with some probability a
 * click, invest and/or conversion fire after randomized delays. The delays are
 * deliberately jittered and staggered so that events routinely arrive late and out
 * of order relative to their event-time, exercising the streaming watermark logic.
 *
 * <p>The event-time ({@code ts}) is the moment the action logically happened, while
 * the actual publish is deferred by a (larger, jittered) processing delay. That gap
 * is exactly the late-arrival the stream processor must tolerate.
 */
@Component
public class FunnelSimulator {

    private static final double P_CLICK = 0.30;
    private static final double P_INVEST = 0.12;
    private static final double P_CONVERSION = 0.05;

    private final EventPublisher publisher;

    public FunnelSimulator(EventPublisher publisher) {
        this.publisher = publisher;
    }

    /** Kick off the (possibly empty) funnel for an impression. Non-blocking. */
    public void simulate(Impression imp) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        if (rnd.nextDouble() < P_CLICK) {
            long eventTs = imp.ts() + rnd.nextLong(200, 3_000);
            scheduleAt(eventTs, () -> publisher.publishClick(
                    new Click(imp.requestId(), imp.memeId(), imp.bidderId(), eventTs)));
        }
        if (rnd.nextDouble() < P_INVEST) {
            long eventTs = imp.ts() + rnd.nextLong(1_000, 6_000);
            long amount = rnd.nextLong(100, 5_000);
            scheduleAt(eventTs, () -> publisher.publishInvest(
                    new Invest(imp.requestId(), imp.memeId(), imp.bidderId(), amount, eventTs)));
        }
        if (rnd.nextDouble() < P_CONVERSION) {
            long eventTs = imp.ts() + rnd.nextLong(2_000, 12_000);
            long value = rnd.nextLong(500, 20_000);
            scheduleAt(eventTs, () -> publisher.publishConversion(
                    new Conversion(imp.requestId(), imp.memeId(), imp.bidderId(), value, eventTs)));
        }
    }

    /**
     * Publish after a processing delay that is intentionally longer (and jittered)
     * than the event-time offset, so publish order != event-time order.
     */
    private void scheduleAt(long eventTs, Runnable emit) {
        long processingDelay = Math.max(0, eventTs - System.currentTimeMillis())
                + ThreadLocalRandom.current().nextLong(50, 500);
        Mono.delay(Duration.ofMillis(processingDelay), Schedulers.parallel())
                .doOnNext(t -> emit.run())
                .subscribe();
    }
}
