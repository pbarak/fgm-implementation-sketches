package fgm.worker.statistics.implementation.state;

import fgm.common.Vector;
import fgm.common.Statistic;

public abstract class FgmStatistic<T extends Vector> implements Statistic<T> {
    private T stateVector;

    public FgmStatistic() {
    }

    public FgmStatistic(T vector) {
        this.stateVector = vector;
    }

    public T getVector() {
        return stateVector;
    }

    public <K extends Vector> void setVector(K vector) {
        if (this.stateVector.getToken().equals(vector.getToken())) {
            this.stateVector = (T) vector; // according to Harrison paper
        } else {
            throw new IllegalArgumentException("setVector token mismatch: " +
                    this.stateVector.getToken() + " vs " + vector.getToken());
        }

    } //guaranteed to be safe according to paper

    public void addVectors(T vector) {
        this.stateVector.add(vector);
    }

    public void scaleVector(Double scalar) {
        this.stateVector.scale(scalar);
    }

}
