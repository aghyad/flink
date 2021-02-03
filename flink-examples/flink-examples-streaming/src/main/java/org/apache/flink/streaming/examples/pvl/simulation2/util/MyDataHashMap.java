package org.apache.flink.streaming.examples.pvl.simulation2.util;

import java.util.HashMap;

public class MyDataHashMap {

    public HashMap<String, Object> dataElement = new HashMap<String, Object>();

    public MyDataHashMap(String value, String partitionKey, int delayInSecs) {
        dataElement.put("value", value);
        dataElement.put("partitionKey", partitionKey);
        dataElement.put("delayInSecs", delayInSecs);
    }

    public String getPartitionKey() {
        return (String) dataElement.get("partitionKey");
    }

    public int getDelayInSecs() {
        return (int) dataElement.get("delayInSecs");
    }

    public String getValue() {
        return (String) dataElement.get("value");
    }

    public void injectDelay() throws InterruptedException {
        Thread.sleep(getDelayInSecs() * 1000);
    }
}
