package org.apache.flink.streaming.examples.pvl.simulation2.deprecated;

public class MyDataTuple {

    private String[] dataElement;

    public MyDataTuple(String... members) {
        this.dataElement = members;
    }

    public String get(int index) {
        return dataElement[index];
    }

    public int size() {
        return dataElement.length;
    }
}
