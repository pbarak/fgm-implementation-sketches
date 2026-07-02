package fgm.worker.statistics.implementation.state;

import fgm.common.Vector;
import fgm.worker.statistics.LocalStatistic;
import fgm.streamdata.StreamRecord;

public class FgmLocalStatistic<T extends Vector> extends FgmStatistic<T> implements LocalStatistic<T> {
    public FgmLocalStatistic() {
        super();
    }

    public FgmLocalStatistic(T vector) {
        super(vector);
    }

    @Override
    public void update(StreamRecord streamRecord, double frequency) {
        this.getVector().update(streamRecord, frequency);
    }

}