package org.apache.flink.streaming.examples.pvl.simulation6.util;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class MyWindowFunction
        implements WindowFunction<MyDataHashMap, String, String, GlobalWindow> {

    private int dumpToSinkSize;

    public MyWindowFunction(int dumpToSinkSize) {
        this.dumpToSinkSize = dumpToSinkSize;
    }

    /**
     * We designed the trigger function in a way that when we hit this evaluation function, we
     * are in situation where the source stopped sending new data elements.
     *
     * This evaluation function takes a window at any time and performs the following:
     *
     * 1- check how many tripIds the data in the window belongs to:
     *      - if one tripID for all, consider all data items
     *      - if more than one tripId, throw away all data items belonging to all tripIds except
     *      the last occurring tripID
     *
     * 2- next, sort data in consideration based on item's eventTime value (ascending order)
     *
     * 3- next, count the sorted data (let's assume that dumpToSinkSize = 10 for example):
     *      - if count > dumpToSinkSize, take the last dumpToSinkSize elements and store it in sink
     *      - else, take all the sorted elements, and store them in sink
     *
     * */
    @Override
    public void apply(
            String s, GlobalWindow window, Iterable<MyDataHashMap> elements, Collector<String> out)
            throws Exception {

        int windowContentSize = 0;
        String windowContent = "";

        for (MyDataHashMap elem : elements) {
            windowContent += " " + elem.getValue();
            windowContentSize++;
        }

        if (windowContentSize == dumpToSinkSize) {
            System.out.printf(
                    "*** streaming [window=%d] *** : %s    ----> STORE DATA\n",
                    windowContentSize, windowContent.trim());
        } else {
            System.out.printf(
                    "*** streaming [window=%d] *** : %s\n",
                    windowContentSize, windowContent.trim());
        }
    }
}
