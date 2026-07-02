package fgm.worker.windowoperator;

import fgm.common.Vector;
import fgm.streamdata.StreamRecord;
import fgm.worker.statistics.LocalStatistic;

public interface Window<T extends Vector> {
    LocalStatistic<T> getLocalStatistic();

    void update(StreamRecord streamRecord, double frequency, Long timestamp);

    void slidePunctuate(long punctuationTS);
}
