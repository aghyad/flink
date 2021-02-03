package org.apache.flink.streaming.examples.pvl.simulation2;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.examples.pvl.simulation2.util.DataDictionary;
import org.apache.flink.streaming.examples.pvl.simulation2.util.MyDataElement;
import org.apache.flink.streaming.examples.pvl.simulation2.util.MyWindowFunction;

public class SlideWindowSimulation2 {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        /*
         * Window Size:
         * - A window is composed of specific number of places equals the size of that window
         * - A place in a window can have 1 or 0 data elements
         * - At any time, a window can't have more nor less than X places, but exactly X places,
         *   where X = window size (integer)
         * - This means that for a window of size 15, there are guaranteed 15 places at any time,
         *   some, none, or even all of which allocate data elements (i.e. the window can have a
         *   min of 0 data elements and a max of 15 data elements in this example)
         *
         * Window Sliding:
         * - Sliding a window means inserting one or more new places into the front of the window,
         *   and at the same time, removing one place at the tail of the window including whatever
         *   occupied that place
         * - Sliding a window happens in isolation from the availability data elements to be
         *   allocated inside the window. This means that when a window slides, there's a
         *   possibility that the new added place(s) to the head is/are empty of any new data
         *   elements
         *
         * Triggering Functions on a Window:
         * - We can configure the settings of a sliding window to trigger executing a custom
         *   function using count of elements in the window or a pre-determined elapsed time value
         * - Triggering can be done:
         *   - either explicitly to become either in-sync or out-of-sync from the frequency of
         *   sliding and/or inserting data elements
         *   - or implicitly which forces triggering to be done everytime the sliding happens,
         *   meaning that triggering is force to be in-sync with sliding frequency
         *
         */

        final int windowSizeInSecs = params.getInt("window", 15);
        final int slideSizeInSecs = params.getInt("slide", 1);
        final String partitionKey = params.get("partitionKey", "VIN123ABC567");
        //        final int submitDelayInSecs = params.getInt("submitDelayInSecs", 1);
        final int dumpToDynamoSize = params.getInt("dumpToDynamoSize", 10);

        // get the default input data
        System.out.println("Executing SlideWindowSimulation2 example with default input data set.");
        System.out.println("Use --input to specify file input.");
        //        DataDictionary dataDictionary = new DataDictionary(partitionKey,
        // submitDelayInSecs);
        DataDictionary dataDictionary = new DataDictionary(partitionKey);
        DataStream<MyDataElement> dataStream = env.fromElements(dataDictionary.getDataList());

        dataStream
                .map(
                        dataElement -> {
                            try {
                                return dataElement.delayedGetValue();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                                return dataElement.value;
                            }
                        })
                .keyBy(dataElement -> "VIN123ABC567")
                .window(
                        SlidingProcessingTimeWindows.of(
                                Time.seconds(windowSizeInSecs), Time.seconds(slideSizeInSecs)))
                .apply(new MyWindowFunction(dumpToDynamoSize));

        // execute program
        env.execute("SlideWindowSimulation2");
    }
}
