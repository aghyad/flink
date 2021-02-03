package org.apache.flink.streaming.examples.pvl.simulation3;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.examples.pvl.simulation3.util.DataDictionary;
import org.apache.flink.streaming.examples.pvl.simulation3.util.MyDataHashMap;
import org.apache.flink.streaming.examples.pvl.simulation3.util.MyWindowFunction;

public class SlideWindowSimulation3 {

    // *************************************************************************
    // Simulating Sliding EventTime Window
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        final int windowSizeInSecs = params.getInt("window", 15);
        final int slideSizeInSecs = params.getInt("slide", 1);
        final String partitionKey = params.get("partitionKey", "VIN123ABC567");
        final int dumpToDynamoSize = params.getInt("dumpToDynamoSize", 10);

        // get the default input data
        System.out.println("Executing SlideWindowSimulation3 example with default input data set.");
        System.out.println("Use --input to specify file input.");

        DataDictionary dataDictionary = new DataDictionary();
        DataStream<MyDataHashMap> dataStream = env.fromElements(dataDictionary.getDataList());

        dataStream
                .map(
                        dataElement -> {
                            try {
                                dataElement.injectDelay();
                                return dataElement;
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                                return dataElement;
                            }
                        })
                .keyBy(dataElement -> dataElement.getPartitionKey())
                .window(
                        SlidingEventTimeWindows.of(
                                Time.seconds(windowSizeInSecs), Time.seconds(slideSizeInSecs)))
                .apply(new MyWindowFunction(dumpToDynamoSize));

        // execute program
        env.execute("SlideWindowSimulation3");
    }
}
