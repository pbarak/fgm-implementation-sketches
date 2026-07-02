package fgm.queryfunction;

import fgm.common.GlobalStatistic;
import fgm.common.Vector;

public interface QueryFunction<T extends Vector> {
    Double computeQuery(GlobalStatistic<T> globalStatistic);

    Double computeQuery(T vector);
}
