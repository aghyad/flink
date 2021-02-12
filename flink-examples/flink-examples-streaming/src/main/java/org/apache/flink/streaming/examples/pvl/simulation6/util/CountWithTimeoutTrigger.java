package org.apache.flink.streaming.examples.pvl.simulation6.util;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.IOException;

public class CountWithTimeoutTrigger<T, W extends Window> extends Trigger<T, W> {

    private final long maxCount, timeoutMs;

    private final ValueStateDescriptor<Long> countDesc =
            new ValueStateDescriptor("count", LongSerializer.INSTANCE, 0L);
    private final ValueStateDescriptor<Long> deadlineDesc =
            new ValueStateDescriptor("deadline", LongSerializer.INSTANCE, Long.MAX_VALUE);

    private CountWithTimeoutTrigger(long maxCount, long timeoutMs) {
        this.maxCount = maxCount;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx)
            throws Exception {
        final ValueState<Long> deadline = ctx.getPartitionedState(deadlineDesc);
        final ValueState<Long> count = ctx.getPartitionedState(countDesc);

        final long currentDeadline = deadline.value();
        final long currentTimeMs = System.currentTimeMillis();

        final long newCount = count.value() + 1;

        if (currentTimeMs >= currentDeadline || newCount >= maxCount) {
            return fire(deadline, count);
        }

        if (currentDeadline == deadlineDesc.getDefaultValue()) {
            final long nextDeadline = currentTimeMs + timeoutMs;
            deadline.update(nextDeadline);
            ctx.registerProcessingTimeTimer(nextDeadline);
        }

        count.update(newCount);

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx)
            throws Exception {
        final ValueState<Long> deadline = ctx.getPartitionedState(deadlineDesc);
        // fire only if the deadline hasn't changed since registering this timer
        if (deadline.value() == time) {
            return fire(deadline, ctx.getPartitionedState(countDesc));
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        final ValueState<Long> deadline = ctx.getPartitionedState(deadlineDesc);
        final long deadlineValue = deadline.value();
        if (deadlineValue != deadlineDesc.getDefaultValue()) {
            ctx.deleteProcessingTimeTimer(deadlineValue);
        }
        deadline.clear();
    }

    private TriggerResult fire(ValueState<Long> deadline, ValueState<Long> count)
            throws IOException {
        deadline.update(Long.MAX_VALUE);
        count.update(0L);
        return TriggerResult.FIRE;
    }

    public static <T, W extends Window> CountWithTimeoutTrigger<T, W> of(
            long maxCount, long timeoutMs) {
        return new CountWithTimeoutTrigger<>(maxCount, timeoutMs);
    }
}
