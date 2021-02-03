package org.apache.flink.streaming.examples.pvl.simulation2.util;

public class DataDictionary {

    private final int[] dataDelayInSecs = {
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 5, 5, 5, 5, 5, 5, 5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 5,
        5, 5, 5, 30
    };
    private final String[] dataArray =
            new String[] {
                "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q",
                "r", "s", "t", "u", "v", "w", "x", "y", "z", "1", "2", "3", "4", "5", "6", "7", "8",
                "%%%"
            };
    private String partitionKey = "VIN123ABC567";
    public MyDataHashMap[] DATA_LIST = new MyDataHashMap[dataArray.length];

    public DataDictionary() {
        for (int i = 0; i < dataArray.length; i++) {
            DATA_LIST[i] = new MyDataHashMap(dataArray[i], partitionKey, dataDelayInSecs[i]);
        }
    }

    public MyDataHashMap[] getDataList() {
        return DATA_LIST;
    }
}
