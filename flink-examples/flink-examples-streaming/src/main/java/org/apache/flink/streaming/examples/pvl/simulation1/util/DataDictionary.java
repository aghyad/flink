package org.apache.flink.streaming.examples.pvl.simulation1.util;

public class DataDictionary {

    private final String[] dataArray =
            new String[] {
                "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q",
                "r", "s", "t"
            };

    public MyDataElement[] DATA_LIST = new MyDataElement[dataArray.length];

    public DataDictionary() {
        for (int i = 0; i < dataArray.length; i++) {
            DATA_LIST[i] = new MyDataElement(dataArray[i]);
        }
    }

    public MyDataElement[] getDataList() {
        return DATA_LIST;
    }
}
