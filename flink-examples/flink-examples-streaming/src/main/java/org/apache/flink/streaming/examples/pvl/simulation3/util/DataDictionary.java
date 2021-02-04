package org.apache.flink.streaming.examples.pvl.simulation3.util;

import java.sql.Timestamp;
import java.util.Calendar;

public class DataDictionary {

    private final String[][] myData =
            new String[][] {
                // { "value", "deliveryDelayInSecs", "partitionKey", "eventTimestampDelayInSecs" },
                new String[] {"M01", "1", "VIN123ABC567", "1"},
                new String[] {"M02", "1", "VIN123ABC567", "2"},
                new String[] {"M03", "1", "VIN123ABC567", "3"},
                new String[] {"M04", "1", "VIN123ABC567", "4"},
                new String[] {"M05", "1", "VIN123ABC567", "5"},
                new String[] {"M06", "1", "VIN123ABC567", "6"},
                new String[] {"M07", "1", "VIN123ABC567", "7"},
                new String[] {"M08", "1", "VIN123ABC567", "8"},
                new String[] {"M09", "1", "VIN123ABC567", "9"},
                new String[] {"M10", "1", "VIN123ABC567", "10"},
                new String[] {"M11", "1", "VIN123ABC567", "11"},
                new String[] {"M12", "1", "VIN123ABC567", "12"},
                new String[] {"M13", "1", "VIN123ABC567", "13"},
                new String[] {"M14", "5", "VIN123ABC567", "14"},
                new String[] {"M15", "5", "VIN123ABC567", "15"},
                new String[] {"M16", "5", "VIN123ABC567", "16"},
                new String[] {"M17", "5", "VIN123ABC567", "17"},
                new String[] {"M18", "5", "VIN123ABC567", "18"},
                new String[] {"M19", "5", "VIN123ABC567", "19"},
                new String[] {"M20", "5", "VIN123ABC567", "20"},
                new String[] {"M21", "1", "VIN123ABC567", "21"},
                new String[] {"M22", "1", "VIN123ABC567", "22"},
                new String[] {"M23", "1", "VIN123ABC567", "23"},
                new String[] {"M24", "1", "VIN123ABC567", "24"},
                new String[] {"M25", "1", "VIN123ABC567", "25"},
                new String[] {"M26", "1", "VIN123ABC567", "26"},
                new String[] {"M27", "1", "VIN123ABC567", "27"},
                new String[] {"M28", "1", "VIN123ABC567", "28"},
                new String[] {"M29", "1", "VIN123ABC567", "29"},
                new String[] {"M30", "1", "VIN123ABC567", "30"},
                new String[] {"M31", "5", "VIN123ABC567", "31"},
                new String[] {"M32", "5", "VIN123ABC567", "32"},
                new String[] {"M33", "5", "VIN123ABC567", "33"},
                new String[] {"M34", "5", "VIN123ABC567", "34"},
                new String[] {"%%%", "30", "VIN123ABC567", "35"},
            };
    public MyDataHashMap[] DATA_LIST = new MyDataHashMap[myData.length];

    public DataDictionary() {
        Timestamp baseTimestamp = new Timestamp(System.currentTimeMillis());

        for (int i = 0; i < myData.length; i++) {
            for (int j = 0; j < myData[0].length; j++) {
                DATA_LIST[i] =
                        new MyDataHashMap(
                                myData[i][0],
                                myData[i][2],
                                Integer.parseInt(myData[i][1]),
                                calculateThisEventTimestamp(
                                        baseTimestamp, Integer.parseInt(myData[i][3])));
            }
        }
    }

    public MyDataHashMap[] getDataList() {
        return DATA_LIST;
    }

    private Timestamp calculateThisEventTimestamp(
            Timestamp baseTimestamp, int eventTimestampDelayInSecs) {
        // calculates the Timestamp value from (baseTimestamp + eventTimestampDelayInSecs)
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(baseTimestamp.getTime());
        cal.add(Calendar.SECOND, eventTimestampDelayInSecs);
        return new Timestamp(cal.getTime().getTime());
    }
}
