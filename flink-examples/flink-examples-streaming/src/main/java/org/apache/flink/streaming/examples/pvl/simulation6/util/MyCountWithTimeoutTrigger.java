package org.apache.flink.streaming.examples.pvl.simulation6.util;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.IOException;

public class MyCountWithTimeoutTrigger<T, W extends Window> extends Trigger<T, W> {

    private final long maxCount, timeoutMs;
    private final ValueStateDescriptor<Long> countDesc =
            new ValueStateDescriptor("count", LongSerializer.INSTANCE, 0L);
    private final ValueStateDescriptor<Long> deadlineDesc =
            new ValueStateDescriptor("deadline", LongSerializer.INSTANCE, Long.MAX_VALUE);
    private final int onProcessingTimeoutInSecs = 1;

    private MyCountWithTimeoutTrigger(long maxCount, long timeoutMs) {
        this.maxCount = maxCount;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx)
            throws Exception {
        final ValueState<Long> deadline = ctx.getPartitionedState(deadlineDesc);
        final long currentDeadline = deadline.value();
        final long currentTimeMs = System.currentTimeMillis();

        String elementValue = ((MyDataHashMap) element).getValue();
        String elementTripId = ((MyDataHashMap) element).getTripId();

        System.out.printf("+ %s [Trip: %s]\n", elementValue, elementTripId);

        if (currentDeadline == deadlineDesc.getDefaultValue()) {
            final long nextProcessingTime = currentTimeMs + (onProcessingTimeoutInSecs * 1000);
            ctx.registerProcessingTimeTimer(nextProcessingTime);
        }

        final long nextDeadline = currentTimeMs + timeoutMs;
        deadline.update(nextDeadline);

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long currentTimeMs, W window, TriggerContext ctx)
            throws Exception {
        final ValueState<Long> deadline = ctx.getPartitionedState(deadlineDesc);
        final long currentDeadline = deadline.value();

        System.out.printf("  waiting %d sec for more data\n", onProcessingTimeoutInSecs);

        // fire only if the deadline hasn't changed since registering this timer
        if (currentTimeMs >= currentDeadline) {
            return fire(deadline, ctx.getPartitionedState(countDesc));
        } else {
            final long nextProcessingTime = currentTimeMs + (onProcessingTimeoutInSecs * 1000);
            ctx.registerProcessingTimeTimer(nextProcessingTime);
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long currentTimeMs, W window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        System.out.println("\n*** clear trigger ...\n");
        final ValueState<Long> deadline = ctx.getPartitionedState(deadlineDesc);
        final long deadlineValue = deadline.value();
        if (deadlineValue != deadlineDesc.getDefaultValue()) {
            ctx.deleteProcessingTimeTimer(deadlineValue);
        }
        deadline.clear();
    }

    private TriggerResult fire(ValueState<Long> deadline, ValueState<Long> count)
            throws IOException {
        System.out.println("\n*** FIRING ...");
        deadline.update(Long.MAX_VALUE);
        return TriggerResult.FIRE;
    }

    public static <T, W extends Window> MyCountWithTimeoutTrigger<T, W> of(
            long maxCount, long timeoutMs) {
        return new MyCountWithTimeoutTrigger<>(maxCount, timeoutMs);
    }
}
