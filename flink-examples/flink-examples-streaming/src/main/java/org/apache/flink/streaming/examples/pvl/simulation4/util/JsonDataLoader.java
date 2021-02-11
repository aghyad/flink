package org.apache.flink.streaming.examples.pvl.simulation4.util;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class JsonDataLoader {

    public ArrayList<String[]> loadDataFromFile(String fileName) {
        JSONParser jsonParser = new JSONParser();

        ArrayList<String[]> myLoadedData = new ArrayList<String[]>();

        try (FileReader reader = new FileReader(fileName)) {
            Object obj = jsonParser.parse(reader);
            JSONArray dataList = (JSONArray) obj;
            for (Object dataItem : dataList) {
                JSONObject jsonObject = (JSONObject) dataItem;
                String[] dataItemLine = parseDataItem(jsonObject);
                myLoadedData.add(dataItemLine);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return myLoadedData;
    }

    private String[] parseDataItem(JSONObject dataItem) {
        JSONObject dataLine = (JSONObject) dataItem.get("data_item");
        String value = (String) dataLine.get("value");
        String deliveryDelay = (String) dataLine.get("delay");
        String partitionKey = (String) dataLine.get("vin");
        String eventTimestamp = (String) dataLine.get("event_timestamp");

        return new String[] {value, deliveryDelay, partitionKey, eventTimestamp};
    }
}
