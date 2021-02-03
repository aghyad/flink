package org.apache.flink.streaming.examples.pvl.simulation1;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.pvl.simulation1.util.DataDictionary;
import org.apache.flink.streaming.examples.pvl.simulation1.util.MyDataElement;
import org.apache.flink.streaming.examples.pvl.simulation1.util.MyProcessWindowFunction;

public class SlideWindowSimulation1 {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        final int windowSize = params.getInt("window", 15);
        final int slideSize = params.getInt("slide", 1);

        // get the default input data
        System.out.println("Executing SlideWindowSimulation1 example with default input data set.");
        System.out.println("Use --input to specify file input.");
        DataDictionary dataDictionary = new DataDictionary();
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
                .countWindow(windowSize, slideSize)
                .process(new MyProcessWindowFunction());

        // execute program
        env.execute("SlideWindowSimulation1");
    }
}
