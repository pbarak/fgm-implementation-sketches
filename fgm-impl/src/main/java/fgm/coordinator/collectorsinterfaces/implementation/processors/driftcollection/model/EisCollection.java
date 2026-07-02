package fgm.coordinator.collectorsinterfaces.implementation.processors.driftcollection.model;

import fgm.common.Vector;

import java.util.ArrayList;
import java.util.List;

public class EisCollection<T extends Vector> {

    //the coordinator maintains, for each site i, an estimated state vector Ei, according to paper.
    private List<EiCollectionEntry<T>> Eis;

    public EisCollection() {
        this.Eis = new ArrayList<>();
    }

    public EiCollectionEntry<T> getEntry(int i) {
        return this.getEis().get(i);
    }

    public List<EiCollectionEntry<T>> getEis() {
        if (this.Eis != null) {
            return this.Eis;
        } else {
            return new ArrayList<>();
        }
    }

    public String getEiNodeId(int i) {
        if (this.Eis != null) {
            return this.getEis().get(i).getEiNodeId();
        }
        return null;
    }

    public void collectEi(DriftVectorEntry<T> driftVectorEntry) {
        this.getEis().add(
                new EiCollectionEntry<>(driftVectorEntry.getDriftNodeId(), driftVectorEntry.getDriftNodeDriftVector()));
    }

}