package fgm.coordinator.collectorsinterfaces.implementation.processors.driftcollection.model;

import fgm.common.Vector;

import java.util.AbstractMap;

public class EiCollectionEntry<T extends Vector> {


    //the string is the node id the FastAGMS is the Local Statistic modified in order to create the global Statistic according to the paper
    private AbstractMap.SimpleEntry<String, T> Ei_Entry;

    public EiCollectionEntry(String nodeId, T nodeLocalStatisticVector) {
        this.Ei_Entry = new AbstractMap.SimpleEntry<>(nodeId, nodeLocalStatisticVector);
    }

    public String getEiNodeId() {
        return this.Ei_Entry.getKey();
    }

    public T getEiLocalStatistic() {
        return this.Ei_Entry.getValue();
    }
}
