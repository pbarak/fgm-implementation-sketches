package fgm.common.statevector;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.List;

import fgm.common.Vector;
import fgm.streamdata.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyFrequencyList implements Vector {
    private static final Logger logger =
            LoggerFactory.getLogger(KeyFrequencyList.class);

    private static final double DEFAULT_FREQUENCY = 1.0d;

    private List<Entry<Long, Double>> stateVector;

    public KeyFrequencyList(List<Entry<Long, Double>> initial) {
        this.stateVector = new ArrayList<>(initial);
    }

    public KeyFrequencyList() {
        this.stateVector = new ArrayList<>();
    }

    public List<Entry<Long, Double>> getStateVector() {
        return stateVector;
    }

    public void setStateVector(List<Entry<Long, Double>> stateVector) {
        this.stateVector = stateVector;
    }

    @Override
    public void scale(Double scalar) {
        for (Entry<Long, Double> e : stateVector) {
            e.setValue(e.getValue() * scalar);
        }
    }

    @Override
    public void update(StreamRecord streamRecord, double frequency) {
        Long key = streamRecord.getKey();
        if (stateVector.isEmpty()) {
            stateVector.add(new SimpleEntry<>(key, frequency));
            return;
        }

        // Try to update an existing entry
        for (int i = 0; i < stateVector.size(); i++) {
            Entry<Long, Double> entry = stateVector.get(i);
            int cmp = Long.compare(key, entry.getKey());

            if (cmp == 0) {
                // update existing
                entry.setValue(entry.getValue() + frequency);
                return;
            }

            if (cmp < 0) {
                // insert before
                stateVector.add(i, new SimpleEntry<>(key, frequency));
                return;
            }
        }

        // else append at end
        stateVector.add(new SimpleEntry<>(key, frequency));
    }

    /**
     * Removes any entries whose frequency dropped to zero.
     */
    public void pack() {
        stateVector.removeIf(e -> e.getValue() == 0);
    }

    /**
     * Ensures the list is sorted by key ascending.
     */
    public void sortStateVector() {
        Collections.sort(stateVector,
                (e1, e2) -> Long.compare(e1.getKey(), e2.getKey()));
    }

    /**
     * Merges another vector into this one (summing frequencies of matching keys).
     */
    public void add(Vector other) {

        if (!(other.getToken().equals(KeyFrequencyList.class.getName()))){
            throw new IllegalArgumentException("Can only add another KeyFrequencyList");
        }

        KeyFrequencyList o = (KeyFrequencyList) other;
        List<Entry<Long, Double>> merged = new ArrayList<>();

        int i = 0, j = 0;
        while (i < stateVector.size() && j < o.stateVector.size()) {
            Entry<Long, Double> a = stateVector.get(i);
            Entry<Long, Double> b = o.stateVector.get(j);

            int cmp = Long.compare(a.getKey(), b.getKey());
            if (cmp < 0) {
                merged.add(new SimpleEntry<>(a.getKey(), a.getValue()));
                i++;
            } else if (cmp > 0) {
                merged.add(new SimpleEntry<>(b.getKey(), b.getValue()));
                j++;
            } else {
                merged.add(new SimpleEntry<>(a.getKey(),
                        a.getValue() + b.getValue()));
                i++;
                j++;
            }
        }
        // tail
        while (i < stateVector.size()) {
            Entry<Long, Double> a = stateVector.get(i++);
            merged.add(new SimpleEntry<>(a.getKey(), a.getValue()));
        }
        while (j < o.stateVector.size()) {
            Entry<Long, Double> b = o.stateVector.get(j++);
            merged.add(new SimpleEntry<>(b.getKey(), b.getValue()));
        }

        this.stateVector = merged;
        this.pack();
    }

    /**
     * Returns a new vector equal to (this – other), dropping non-positives.
     */
    @Override
    public Vector subtract(Vector other) {

        if (!(other.getToken().equals(KeyFrequencyList.class.getName()))) {
            throw new IllegalArgumentException("Can only subtract another KeyFrequencyList");
        }

        KeyFrequencyList o = (KeyFrequencyList) other;
        List<Entry<Long, Double>> result = new ArrayList<>();

        int i = 0, j = 0;
        while (i < stateVector.size() && j < o.stateVector.size()) {
            Entry<Long, Double> a = stateVector.get(i);
            Entry<Long, Double> b = o.stateVector.get(j);

            int cmp = Long.compare(a.getKey(), b.getKey());
            if (cmp < 0) {
                result.add(new SimpleEntry<>(a.getKey(), a.getValue()));
                i++;
            } else if (cmp > 0) {
                j++;
            } else {
                double diff = a.getValue() - b.getValue();
                if (diff > 0) {
                    result.add(new SimpleEntry<>(a.getKey(), diff));
                }
                i++;
                j++;
            }
        }
        // remaining from this
        while (i < stateVector.size()) {
            Entry<Long, Double> a = stateVector.get(i++);
            result.add(new SimpleEntry<>(a.getKey(), a.getValue()));
        }
        this.pack();
        return new KeyFrequencyList(result);
    }

    public int size() {
        return stateVector.size();
    }


    @Override
    public Vector deepCopy() {
        List<Entry<Long, Double>> copy = new ArrayList<>(stateVector.size());
        for (Entry<Long, Double> e : stateVector) {
            copy.add(new SimpleEntry<>(e.getKey(), e.getValue()));
        }
        return new KeyFrequencyList(copy);
    }

    public String getToken() {
        return KeyFrequencyList.class.getName();
    }
}
