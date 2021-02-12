package org.apache.flink.streaming.examples.pvl.simulation6.util;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MyWindowFunction
        implements WindowFunction<MyDataHashMap, String, String, GlobalWindow> {

    private int dumpToSinkSize;

    public MyWindowFunction(int dumpToSinkSize) {
        this.dumpToSinkSize = dumpToSinkSize;
    }

    /**
     * We designed the trigger function in a way that when we hit this evaluation function, we are
     * in situation where the source stopped sending new data elements.
     *
     * <p>This evaluation function takes a window at any time and performs the following:
     *
     * <p>1- check how many tripIds the data in the window belongs to: - if one tripID for all,
     * consider all data items - if more than one tripId, throw away all data items belonging to all
     * tripIds except the last occurring tripID
     *
     * <p>2- next, sort data in consideration based on item's eventTime value (ascending order)
     *
     * <p>3- next, count the sorted data (let's assume that dumpToSinkSize = 10 for example): - if
     * count > dumpToSinkSize, take the last dumpToSinkSize elements and store it in sink - else,
     * take all the sorted elements, and store them in sink
     */
    @Override
    public void apply(
            String s, GlobalWindow window, Iterable<MyDataHashMap> elements, Collector<String> out)
            throws Exception {
        Iterable<MyDataHashMap> filteredElements = filterElementsWithLastTripId(elements);

        filteredElements = sortElementsBasedOnEventTime(filteredElements);

        int windowContentSize = 0;
        String windowContent = "";
        for (MyDataHashMap elem : filteredElements) {
            windowContent += " " + elem.getValue();
            windowContentSize++;
        }

        if (windowContentSize > dumpToSinkSize) {
            Iterable<MyDataHashMap> subListToStore =
                    ((ArrayList<MyDataHashMap>) filteredElements)
                            .subList(windowContentSize - dumpToSinkSize, windowContentSize);

            String subWindowContent = "";
            int subWindowContentSize = 0;
            for (MyDataHashMap elem : subListToStore) {
                subWindowContent += " " + elem.getValue();
                subWindowContentSize++;
            }

            System.out.printf(
                    "*** streaming [window=%d] *** : %s    ----> STORE Subwindow [window=%d] %s\n",
                    windowContentSize,
                    windowContent.trim(),
                    subWindowContentSize,
                    subWindowContent.trim());
        } else {
            System.out.printf(
                    "*** streaming [window=%d] *** : %s    ----> STORE ALL WINDOW\n",
                    windowContentSize, windowContent.trim());
        }
    }

    private Iterable<MyDataHashMap> sortElementsBasedOnEventTime(Iterable<MyDataHashMap> elements) {
        Comparator<MyDataHashMap> myEventTimeComparator =
                (MyDataHashMap a, MyDataHashMap b) -> {
                    return a.getEventTimestamp().compareTo(b.getEventTimestamp());
                };

        Collections.sort((List<MyDataHashMap>) elements, myEventTimeComparator);

        return elements;
    }

    private Iterable<MyDataHashMap> filterElementsWithLastTripId(Iterable<MyDataHashMap> elements) {
        ArrayList<String> filteredListOfTripsIds = filterTripIdsFrom(elements);
        String lastTripId = filteredListOfTripsIds.get(filteredListOfTripsIds.size() - 1);

        ArrayList<MyDataHashMap> filteredElements = new ArrayList<MyDataHashMap>();

        for (MyDataHashMap elem : elements)
            if (elem.getTripId().equals(lastTripId)) filteredElements.add(elem);

        return filteredElements;
    }

    private ArrayList<String> filterTripIdsFrom(Iterable<MyDataHashMap> elements) {
        ArrayList<String> tripIds = new ArrayList<String>();

        for (MyDataHashMap elem : elements) {
            if (!tripIds.contains(elem.getTripId())) {
                tripIds.add(elem.getTripId());
            }
        }

        return tripIds;
    }
}
