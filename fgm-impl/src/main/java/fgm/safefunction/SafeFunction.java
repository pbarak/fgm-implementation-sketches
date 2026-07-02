package fgm.safefunction;

import fgm.common.Vector;

import java.io.Serializable;

public interface SafeFunction<T extends Vector> extends Serializable {
    double computePhi(T drift, T E, double T);

    double computePhiWithZeroDrift(T E, Double T_val);

}
