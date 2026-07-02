package fgm.common.statevector;

import fgm.common.Vector;
import fgm.streamdata.StreamRecord;

import java.util.HashMap;
import java.util.Map;

public class HashMapState implements Vector {
    private final Map<Long, Double> state;


    public HashMapState(Map<Long, Double> state) {
        this.state = state;
    }

    public HashMapState() {
        this.state = new HashMap<>();
    }

    public HashMapState(HashMapState map) {
        this.state = new HashMap<>(map.getState());
    }

    public static HashMapState empty() {
        return new HashMapState(new HashMap<>());
    }

    public Map<Long, Double> getState() {
        return state;
    }

    @Override
    public void scale(Double scalar) {
        for (Map.Entry<Long, Double> entry : state.entrySet()) {
            Double scaled = entry.getValue() * scalar;
            entry.setValue(scaled);
        }
    }

    @Override
    public void update(StreamRecord streamRecord, double frequency) {
        Long key = streamRecord.getKey();
        // If the key is already present, update its value
        if (state.containsKey(key)) {
            state.put(key, state.get(key) + frequency);
        } else {
            // Otherwise, add a new entry with the given frequency
            state.put(key, frequency);
        }
    }


    public void update(Long key, Double frequency) {
        // If the key is already present, update its value
        if (state.containsKey(key)) {
            state.put(key, state.get(key) + frequency);
        } else {
            // Otherwise, add a new entry with the given frequency
            state.put(key, frequency);
        }
    }

    @Override
    public void add(Vector vector) {
        if (!(vector.getToken().equals(HashMapState.class.getName()))){
            throw new IllegalArgumentException("Can only add another HashMapState");
        }
        HashMapState other = (HashMapState) vector;
        // merge every (key, value) from the other state into this.state
        other.state.forEach((key, value) ->
                state.merge(key, value, Double::sum)
        );
    }

    @Override
    public HashMapState subtract(Vector vector) {
        if (!(vector.getToken().equals(HashMapState.class.getName()))) {
            throw new IllegalArgumentException("Can only subtract another HashMapState");
        }
        // Deep-copy this, then subtract into the copy:
        HashMapState result = this.deepCopy();
        HashMapState other = (HashMapState) vector;
        other.state.forEach((key, value) ->
                result.state.merge(key, -value, (oldV, negV) -> oldV + negV)
        );
        return result;
    }

    @Override
    public HashMapState deepCopy() {
        HashMap<Long, Double> stateCopy = new HashMap<>(this.state);
        return new HashMapState(stateCopy);
    }

    @Override
    public int hashCode() {
        return state.hashCode();
    }

    public String getToken(){
        return HashMapState.class.getName();
    }
}
