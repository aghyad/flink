package org.apache.flink.streaming.examples.pvl.simulation2.util;

public class MyDataElement {

    public String value;
    public String partitionKey;
    private int delayInSecs;

    public MyDataElement(String value, String partitionKey, int delayInSecs) {
        this.value = value;
        this.partitionKey = partitionKey;
        this.delayInSecs = delayInSecs;
    }

    public String delayedGetValue() throws InterruptedException {
        Thread.sleep(this.delayInSecs * 1000);
        return this.value;
    }
}
