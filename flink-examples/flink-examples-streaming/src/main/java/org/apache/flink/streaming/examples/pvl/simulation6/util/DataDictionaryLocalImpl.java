package org.apache.flink.streaming.examples.pvl.simulation6.util;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;

public class DataDictionaryLocalImpl implements DataDictionary {

    public String fileName;
    public ArrayList<String[]> myData;
    public int myDataLength = 0;
    public MyDataHashMap[] DATA_LIST;

    public DataDictionaryLocalImpl(String fileName) {
        this.fileName = fileName;
        myData = new DataLoader().loadDataFromFile(fileName);
        for (String[] stringArray : myData) {
            myDataLength++;
        }
        DATA_LIST = new MyDataHashMap[myDataLength];

        Timestamp baseTimestamp = new Timestamp(System.currentTimeMillis());

        for (int i = 0; i < myDataLength; i++) {
            for (int j = 0; j < myData.get(0).length; j++) {
                DATA_LIST[i] =
                        new MyDataHashMap(
                                myData.get(i)[0],
                                myData.get(i)[2],
                                Integer.parseInt(myData.get(i)[1]),
                                calculateThisEventTimestamp(
                                        baseTimestamp, Integer.parseInt(myData.get(i)[3])));
            }
        }
    }

    @Override
    public MyDataHashMap[] getDataList() {
        return DATA_LIST;
    }

    @Override
    public Timestamp calculateThisEventTimestamp(
            Timestamp baseTimestamp, int eventTimestampDelayInSecs) {
        // calculates the Timestamp value from (baseTimestamp + eventTimestampDelayInSecs)
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(baseTimestamp.getTime());
        cal.add(Calendar.SECOND, eventTimestampDelayInSecs);
        return new Timestamp(cal.getTime().getTime());
    }
}
