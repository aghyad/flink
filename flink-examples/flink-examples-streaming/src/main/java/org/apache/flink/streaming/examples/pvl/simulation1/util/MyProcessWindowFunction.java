package org.apache.flink.streaming.examples.pvl.simulation1.util;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class MyProcessWindowFunction
        extends ProcessWindowFunction<String, String, String, GlobalWindow> {
    @Override
    public void process(String s, Context context, Iterable<String> elements, Collector<String> out)
            throws Exception {
        System.out.println(String.join("-", elements));
    }
}
