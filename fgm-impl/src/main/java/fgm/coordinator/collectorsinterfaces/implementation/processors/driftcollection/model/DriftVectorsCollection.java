package fgm.coordinator.collectorsinterfaces.implementation.processors.driftcollection.model;

import fgm.common.Vector;
import fgm.common.messages.DriftVectorMessage;

import java.util.ArrayList;
import java.util.List;

public class DriftVectorsCollection<T extends Vector> {

    private List<DriftVectorEntry<T>> drifts;

    public DriftVectorsCollection() {
        this.drifts = new ArrayList<>();
    }

    public List<DriftVectorEntry<T>> getDrifts() {
        if (this.drifts != null) {
            return this.drifts;
        } else {
            return new ArrayList<>();
        }
    }

    public DriftVectorEntry<T> getEntry(int i) {
        return this.getDrifts().get(i);
    }

    public void collectDrift(DriftVectorMessage driftVectorMessage) {
        @SuppressWarnings("unchecked")  // deserialization boundary: the wire vector arrives token-less
        T stateVector = (T) driftVectorMessage.getStateVector(); //todo
        this.getDrifts().add(
                new DriftVectorEntry<>(driftVectorMessage.getSiteId(), stateVector));
    }

}