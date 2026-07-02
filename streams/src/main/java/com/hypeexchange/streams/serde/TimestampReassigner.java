package com.hypeexchange.streams.serde;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

import java.util.function.ToLongFunction;

/**
 * Rewrites each record's stream-time to a value-derived event-time before it flows
 * into a downstream windowed aggregation. Used so attributed clicks are counted in
 * their impression's window rather than the (later) window they arrive in.
 */
public class TimestampReassigner<K, V> implements ProcessorSupplier<K, V, K, V> {

    private final ToLongFunction<V> tsFn;

    public TimestampReassigner(ToLongFunction<V> tsFn) {
        this.tsFn = tsFn;
    }

    @Override
    public Processor<K, V, K, V> get() {
        return new Processor<>() {
            private ProcessorContext<K, V> context;

            @Override
            public void init(ProcessorContext<K, V> context) {
                this.context = context;
            }

            @Override
            public void process(Record<K, V> record) {
                if (record.value() == null) {
                    context.forward(record);
                    return;
                }
                context.forward(record.withTimestamp(tsFn.applyAsLong(record.value())));
            }
        };
    }
}
