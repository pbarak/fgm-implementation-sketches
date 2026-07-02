package fgm.coordinator.collectorsinterfaces.implementation.processors.state;

import fgm.worker.statistics.implementation.state.FgmStatistic;
import fgm.common.GlobalStatistic;
import fgm.common.Vector;

public class FgmGlobalStatistic<T extends Vector> extends FgmStatistic<T> implements GlobalStatistic<T> {
    public FgmGlobalStatistic(T vector) {
        super(vector);
    }

}
