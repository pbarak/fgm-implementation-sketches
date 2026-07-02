package fgm.common;

import java.io.Serializable;

public interface Statistic<T extends Vector> extends Serializable {
    T getVector();

    <K extends Vector> void setVector(K vector);

    void addVectors(T vector);

    void scaleVector(Double scalar);

}
