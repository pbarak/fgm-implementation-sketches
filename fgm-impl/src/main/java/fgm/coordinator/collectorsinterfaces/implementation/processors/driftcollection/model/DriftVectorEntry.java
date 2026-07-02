package fgm.coordinator.collectorsinterfaces.implementation.processors.driftcollection.model;

import fgm.common.Vector;

import java.util.AbstractMap;

public class DriftVectorEntry<T extends Vector> {

    //the string is the node id the FastAGMS is the Node DrifVector collected from the Coordinator
    private AbstractMap.SimpleEntry<String, T> driftEntry;

    public DriftVectorEntry(String nodeId, T nodeDriftVector) {
        this.driftEntry = new AbstractMap.SimpleEntry<>(nodeId, nodeDriftVector);
    }

    public String getDriftNodeId() {
        return this.driftEntry.getKey();
    }

    public T getDriftNodeDriftVector() {
        return this.driftEntry.getValue();
    }
}
