package org.apache.flink.streaming.examples.pvl.simulation4.util;

import java.sql.Timestamp;

public interface DataDictionary {

    public MyDataHashMap[] getDataList();

    public Timestamp calculateThisEventTimestamp(
            Timestamp baseTimestamp, int eventTimestampDelayInSecs);
}
