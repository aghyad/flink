package org.apache.flink.streaming.examples.pvl.simulation6;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.pvl.simulation6.util.DataDictionaryLocalImpl;
import org.apache.flink.streaming.examples.pvl.simulation6.util.MyCountWithTimeoutTrigger;
import org.apache.flink.streaming.examples.pvl.simulation6.util.MyDataHashMap;
import org.apache.flink.streaming.examples.pvl.simulation6.util.MyWindowFunction;

public class SlideWindowSimulation6 {

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
                        "/Users/aghyad.saleh/Java/projects/flink/flink/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/pvl/simulation6/docs/data.txt");
        final int windowSize = params.getInt("window", 15);
        final int slideSize = params.getInt("slide", 1);
        final int maxTimeoutInSecs = params.getInt("maxTimeout", 5);
        final int dumpToSinkSize = params.getInt("dumpToSinkSize", 10);

        // get the default input data
        System.out.println("Executing SlideWindowSimulation4 example with default input data set.");
        System.out.println("Use --input to specify file input.");

        DataDictionaryLocalImpl dataDictionary = new DataDictionaryLocalImpl(dataFileName);

        for (MyDataHashMap elem : dataDictionary.getDataList()) {
            System.out.println(elem.getValue() + " ---> " + elem.getEventTimestamp().toString());
        }

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
                .countWindow(windowSize, slideSize)
                .trigger(MyCountWithTimeoutTrigger.of(windowSize, maxTimeoutInSecs * 1000))
                .apply(new MyWindowFunction(dumpToSinkSize));

        // execute program
        env.execute("SlideWindowSimulation6");
    }
}
