package org.apache.flink.streaming.examples.pvl.simulation5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.examples.pvl.simulation5.util.DataDictionaryLocalImpl;
import org.apache.flink.streaming.examples.pvl.simulation5.util.MyDataHashMap;
import org.apache.flink.streaming.examples.pvl.simulation5.util.MyWindowFunction;

import java.time.Duration;

public class SlideWindowSimulation5 {

    // *************************************************************************
    // Simulating Sliding EventTime Window
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        final String dataSource = params.get("source", "local");
        final String dataFileName =
                params.get(
                        "file",
                        "/Users/aghyad.saleh/Java/projects/flink/flink/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/pvl/simulation5/docs/data.txt");
        final int windowSizeInSecs = params.getInt("window", 15);
        final int slideSizeInSecs = params.getInt("slide", 1);
        final int dumpToDynamoSize = params.getInt("dumpToDynamoSize", 10);

        // get the default input data
        System.out.println("Executing SlideWindowSimulation4 example with default input data set.");
        System.out.println("Use --input to specify file input.");

        DataDictionaryLocalImpl dataDictionary = new DataDictionaryLocalImpl(dataFileName);

        for (MyDataHashMap elem : dataDictionary.getDataList()) {
            System.out.println(elem.getValue() + " ---> " + elem.getEventTimestamp().toString());
        }

        WatermarkStrategy myWatermarkStrategy =
                WatermarkStrategy.<MyDataHashMap>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                (dataElement, timestamp) ->
                                        dataElement.getEventTimestamp().getTime());

        DataStream<MyDataHashMap> dataStream =
                env.fromElements(dataDictionary.getDataList())
                        .assignTimestampsAndWatermarks(myWatermarkStrategy);

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
        env.execute("SlideWindowSimulation5");
    }
}
