package fgm.coordinator.collectorsinterfaces.implementation.processors.state;

import fgm.common.messages.PhiMessage;

import java.util.AbstractMap;
import java.util.ArrayList;

public class PhiMessageCollection {
    private final ArrayList<AbstractMap.SimpleEntry<String, Double>> phis = new ArrayList<>();

    public PhiMessageCollection() {
    }

    public void add(PhiMessage phiMessage) {

        if (this.phis.size() == 0) {
            this.phis.add(new AbstractMap.SimpleEntry<>(phiMessage.getSiteId(), phiMessage.getPhiValue()));
        } else {
            boolean found = false;
            for (AbstractMap.SimpleEntry<String, Double> record : this.phis) {
                if (record.getKey().equals(phiMessage.getSiteId())) {
                    found = true;
                }
            }
            if (!found) {
                this.phis.add(new AbstractMap.SimpleEntry<>(phiMessage.getSiteId(), phiMessage.getPhiValue()));
            }
        }
    }

    public int size() {
        return this.phis.size();
    }

    public double calculatePhi() {
        double psi = 0.0d;
        for (AbstractMap.SimpleEntry<String, Double> record : this.phis) {
            psi = psi + record.getValue().doubleValue();
        }
        this.phis.clear();
        return psi;
    }
}

