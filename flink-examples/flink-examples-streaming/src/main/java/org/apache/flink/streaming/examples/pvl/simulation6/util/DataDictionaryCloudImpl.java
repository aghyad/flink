package org.apache.flink.streaming.examples.pvl.simulation6.util;

import java.sql.Timestamp;

public class DataDictionaryCloudImpl implements DataDictionary {

    @Override
    public MyDataHashMap[] getDataList() {
        return null;
    }

    @Override
    public Timestamp calculateThisEventTimestamp(
            Timestamp baseTimestamp, int eventTimestampDelayInSecs) {
        return null;
    }
}
