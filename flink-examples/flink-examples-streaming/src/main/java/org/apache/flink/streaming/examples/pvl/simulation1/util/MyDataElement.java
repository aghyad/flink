package org.apache.flink.streaming.examples.pvl.simulation1.util;

public class MyDataElement {

    public String value;
    private final int sleepLimitInSecs = 1;

    public MyDataElement(String value) {
        this.value = value;
    }

    public String delayedGetValue() throws InterruptedException {
        Thread.sleep(sleepLimitInSecs * 1000);
        return this.value;
    }
}
