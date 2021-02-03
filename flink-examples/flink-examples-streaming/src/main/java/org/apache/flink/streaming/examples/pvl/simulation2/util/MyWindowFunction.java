package org.apache.flink.streaming.examples.pvl.simulation2.util;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyWindowFunction implements WindowFunction<MyDataHashMap, String, String, TimeWindow> {

    private int dumpToDynamoSize;

    public MyWindowFunction(int dumpToDynamoSize) {
        this.dumpToDynamoSize = dumpToDynamoSize;
    }

    @Override
    public void apply(
            String s, TimeWindow window, Iterable<MyDataHashMap> elements, Collector<String> out)
            throws Exception {

        int windowContentSize = 0;
        String windowContent = "";

        for (MyDataHashMap elem : elements) {
            windowContent += " " + elem.getValue();
            windowContentSize++;
        }

        if (windowContentSize == dumpToDynamoSize) {
            System.out.printf(
                    "*** STORE DATA [window=%d] *** : %s\n",
                    windowContentSize, windowContent.trim());
        } else {
            System.out.printf(
                    "*** streaming [window=%d] *** : %s\n",
                    windowContentSize, windowContent.trim());
        }
    }
}
