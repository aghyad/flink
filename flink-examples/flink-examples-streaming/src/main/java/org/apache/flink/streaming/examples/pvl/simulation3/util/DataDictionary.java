package org.apache.flink.streaming.examples.pvl.simulation3.util;

public class DataDictionary {

    private final String[][] myData =
            new String[][] {
                // { "value", "delayInSecs", "partitionKey" },
                new String[] {"M01", "1", "VIN123ABC567"},
                new String[] {"M02", "1", "VIN123ABC567"},
                new String[] {"M03", "1", "VIN123ABC567"},
                new String[] {"M04", "1", "VIN123ABC567"},
                new String[] {"M05", "1", "VIN123ABC567"},
                new String[] {"M06", "1", "VIN123ABC567"},
                new String[] {"M07", "1", "VIN123ABC567"},
                new String[] {"M08", "1", "VIN123ABC567"},
                new String[] {"M09", "1", "VIN123ABC567"},
                new String[] {"M10", "1", "VIN123ABC567"},
                new String[] {"M11", "1", "VIN123ABC567"},
                new String[] {"M12", "1", "VIN123ABC567"},
                new String[] {"M13", "1", "VIN123ABC567"},
                new String[] {"M14", "5", "VIN123ABC567"},
                new String[] {"M15", "5", "VIN123ABC567"},
                new String[] {"M16", "5", "VIN123ABC567"},
                new String[] {"M17", "5", "VIN123ABC567"},
                new String[] {"M18", "5", "VIN123ABC567"},
                new String[] {"M19", "5", "VIN123ABC567"},
                new String[] {"M20", "5", "VIN123ABC567"},
                new String[] {"M21", "1", "VIN123ABC567"},
                new String[] {"M22", "1", "VIN123ABC567"},
                new String[] {"M23", "1", "VIN123ABC567"},
                new String[] {"M24", "1", "VIN123ABC567"},
                new String[] {"M25", "1", "VIN123ABC567"},
                new String[] {"M26", "1", "VIN123ABC567"},
                new String[] {"M27", "1", "VIN123ABC567"},
                new String[] {"M28", "1", "VIN123ABC567"},
                new String[] {"M29", "1", "VIN123ABC567"},
                new String[] {"M30", "1", "VIN123ABC567"},
                new String[] {"M31", "5", "VIN123ABC567"},
                new String[] {"M32", "5", "VIN123ABC567"},
                new String[] {"M33", "5", "VIN123ABC567"},
                new String[] {"M34", "5", "VIN123ABC567"},
                new String[] {"%%%", "30", "VIN123ABC567"},
            };
    public MyDataHashMap[] DATA_LIST = new MyDataHashMap[myData.length];

    public DataDictionary() {
        for (int i = 0; i < myData.length; i++) {
            for (int j = 0; j < myData[0].length; j++) {
                DATA_LIST[i] =
                        new MyDataHashMap(
                                myData[i][0], myData[i][2], Integer.parseInt(myData[i][1]));
            }
        }
    }

    public MyDataHashMap[] getDataList() {
        return DATA_LIST;
    }
}
