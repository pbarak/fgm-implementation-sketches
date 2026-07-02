package fgm.worker.windowoperator.implementation;

import fgm.common.Vector;
import fgm.streamdata.implementation.stream.window.StreamWindowRecord;
import fgm.worker.statistics.LocalStatistic;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class CustomWindowBase<T extends Vector> {

    protected static final double TURNSTILE_FREQ = -1.0d;
    protected int windowWidth; //duration is in seconds
    protected ConcurrentLinkedDeque<List<StreamWindowRecord>> deque;
    protected List<StreamWindowRecord> slideRecords; //each record key is collected until the slide timer fires
    protected StreamWindowRecord last;
    protected List<StreamWindowRecord> lastListInserted;
    protected LocalStatistic<T> localStatistic;
}
