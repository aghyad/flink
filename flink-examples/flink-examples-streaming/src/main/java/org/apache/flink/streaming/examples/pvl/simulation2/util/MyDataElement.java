package org.apache.flink.streaming.examples.pvl.simulation2.util;

public class MyDataElement {

    public String value;
    public String partitionKey;
    private int sleepLimitInSecs;

    public MyDataElement(String value, String partitionKey, int sleepLimitInSecs) {
        this.value = value;
        this.partitionKey = partitionKey;
        this.sleepLimitInSecs = sleepLimitInSecs;
    }

    public String delayedGetValue() throws InterruptedException {
        Thread.sleep(this.sleepLimitInSecs * 1000);
        return this.value;
    }
}
