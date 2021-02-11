package org.apache.flink.streaming.examples.pvl.simulation5.util;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataLoader {

    public ArrayList<String[]> loadDataFromFile(String fileName) {
        final ArrayList<String[]> myLoadedData = new ArrayList<String[]>();

        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {

            List<String> list = stream.collect(Collectors.toList());
            ArrayList<String> arrayList = new ArrayList<String>(list);

            for (String oneLine : arrayList) {
                myLoadedData.add(parseLineToArray(oneLine));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return myLoadedData;
    }

    private String[] parseLineToArray(String oneLine) {
        String[] dataArray = oneLine.split("\\s*,\\s*");
        return dataArray;
    }
}
