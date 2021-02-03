package org.apache.flink.streaming.examples.pvl.simulation2.util;

public class DataDictionary {

    private String partitionKey;
    //    private int submitDelayInSecs;

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

    public MyDataElement[] DATA_LIST = new MyDataElement[dataArray.length];

    //    public DataDictionary(String partitionKey, int submitDelayInSecs) {
    public DataDictionary(String partitionKey) {
        this.partitionKey = partitionKey;
        //        this.submitDelayInSecs = submitDelayInSecs;

        for (int i = 0; i < dataArray.length; i++) {
            DATA_LIST[i] =
                    new MyDataElement(dataArray[i], this.partitionKey, dataDelayInSecs[i]);
        }
    }

    public MyDataElement[] getDataList() {
        return DATA_LIST;
    }
}
