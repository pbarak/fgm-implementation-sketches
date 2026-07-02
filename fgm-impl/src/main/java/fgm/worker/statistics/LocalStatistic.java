package fgm.worker.statistics;

import fgm.common.Statistic;
import fgm.common.Vector;
import fgm.streamdata.StreamRecord;

public interface LocalStatistic<T extends Vector> extends Statistic<T> {
    void update(StreamRecord streamRecord, double frequency);

}