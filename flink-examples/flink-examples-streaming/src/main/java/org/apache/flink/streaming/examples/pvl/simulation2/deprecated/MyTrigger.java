package org.apache.flink.streaming.examples.pvl.simulation2.deprecated;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.examples.pvl.simulation2.util.MyDataHashMap;

public class MyTrigger extends Trigger<MyDataHashMap, TimeWindow> {

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
            throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onElement(
            MyDataHashMap element, long timestamp, TimeWindow window, TriggerContext ctx)
            throws Exception {
        return null;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx)
            throws Exception {
        return null;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {}
}
