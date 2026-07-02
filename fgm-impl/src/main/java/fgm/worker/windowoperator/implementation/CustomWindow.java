package fgm.worker.windowoperator.implementation;


import fgm.common.Vector;
import fgm.streamdata.implementation.stream.window.StreamWindowRecord;
import fgm.factory.specs.IFgmFactory;
import fgm.worker.statistics.implementation.state.FgmLocalStatistic;
import fgm.worker.windowoperator.Window;
import fgm.streamdata.StreamRecord;
import fgm.worker.statistics.LocalStatistic;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CustomWindow<T extends Vector> extends CustomWindowBase<T> implements Window<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomWindow.class);

    private static final Comparator<StreamWindowRecord> BY_TS =
    (a, b) -> Long.compare(a.getTimeStamp(), b.getTimeStamp());

    public CustomWindow(IFgmFactory<T> factory) {
        deque = new ConcurrentLinkedDeque<>();
        slideRecords = new ArrayList<>();
        localStatistic = factory.getLocalStatistic();
        this.windowWidth = factory.getBaseCfg().getWindowSecondsWidth();
    }

    public CustomWindow(T vector, int windowWidth) {
        deque = new ConcurrentLinkedDeque<>();
        slideRecords = new ArrayList<>();
        localStatistic = new FgmLocalStatistic<>(vector);
        this.windowWidth = windowWidth;
    }

    public void slidePunctuate(long streamTimeMs) {

            //extract only records that belong to the current slide or earlier
            List<StreamWindowRecord> recordsForThisSlide = extractRecordsUpToTimestamp(streamTimeMs);

            //process only the extracted records
            if(!recordsForThisSlide.isEmpty()) {
                ingestIncomingSlideRecords(recordsForThisSlide);
            }

            //evict expired records based on window width
            evictExpired(streamTimeMs); // trim by time
    }

    // extract records up to the given timestamp, keeping future records in slideRecords
    private List<StreamWindowRecord> extractRecordsUpToTimestamp(long maxTimestamp){
        List<StreamWindowRecord> recordsToProcess = new ArrayList<>();
        List<StreamWindowRecord> recordsToKeep = new ArrayList<>();

        for(StreamWindowRecord record : slideRecords){
            if(record.getTimeStamp() <= maxTimestamp){
                recordsToProcess.add(record);
            } else {
                // keep records with timestamps beyond current slide boundary
                recordsToKeep.add(record);
            }
        }

        //update slideRecords to only contain future records
        slideRecords = recordsToKeep;

        return recordsToProcess;
    }

    //the ordering key of the record already exists in the queue (window)
    private void updateStateAndAppendToWindow(StreamWindowRecord streamRecord) {
        localStatistic.update(streamRecord, streamRecord.getFrequency());
        lastListInserted.add(streamRecord);
    }


    // the ordering key -- the timestamp epoch second -- already exists in the queue (window)
    private void updateStateAndWindow(StreamWindowRecord rd) {
        this.localStatistic.update(rd, rd.getFrequency());
        List<StreamWindowRecord> recordList = new LinkedList<>();
        recordList.add(rd);
        deque.addLast(recordList);
        last = rd;
        lastListInserted = recordList;
    }

    //the length of the queue shapes the window size, and each record of the queue is a list of the records
    // that have the same timestamp (the timestamp of stream record congestion time according to an external system clock)
    public void update(StreamRecord streamRecord, double frequency, Long timestamp) {
        long epochSeconds = timestamp / 1000L;
        slideRecords.add(new StreamWindowRecord(streamRecord, frequency, epochSeconds, timestamp.longValue()));
    }

    public LocalStatistic<T> getLocalStatistic() {
        return this.localStatistic;
    }


    // accept a specific list of records to process according to slide
    private void ingestIncomingSlideRecords(List<StreamWindowRecord> recordsToIngest) {
        //Sort the records we're about to ingest
        recordsToIngest.sort(BY_TS);
        for (StreamWindowRecord rd : recordsToIngest) {
            if (deque.isEmpty()) {
                updateStateAndWindow(rd);
            } else if (last.getSecondsPreciseTimestamp().equals(rd.getSecondsPreciseTimestamp())) {
                updateStateAndAppendToWindow(rd);
            } else if (last.getSecondsPreciseTimestamp().compareTo(rd.getSecondsPreciseTimestamp()) < 0) {
                updateStateAndWindow(rd);
            }
        }
    }

    private long windowWidthMs() {
        return windowWidth * 1000L;
    }

    private void evictExpired(long nowMs) {
        final long cutoffMs = nowMs - windowWidthMs();

        while (!deque.isEmpty()) {
            List<StreamWindowRecord> head = deque.peekFirst();

            // If the newest record in the head bucket is cutoff, drop whole bucket
            long newestInHead = head.get(head.size() - 1).getTimeStamp();
            if (newestInHead <= cutoffMs) {
                deque.pollFirst();
                for (StreamWindowRecord r : head) {
                    localStatistic.update(r, TURNSTILE_FREQ); // one-by-one, exact inverse of your +1.0 updates
                }
                continue;
            }

            // Partial overlap: remove only the expired records from the head bucket
            Iterator<StreamWindowRecord> it = head.iterator();
            boolean removedAny = false;
            while (it.hasNext()) {
                StreamWindowRecord r = it.next();
                if (r.getTimeStamp() <= cutoffMs) {
                    localStatistic.update(r, TURNSTILE_FREQ);
                    it.remove();
                    removedAny = true;
                } else {
                    break; // the rest are newer
                }
            }

            if (head.isEmpty()) {
                deque.pollFirst();
                continue;
            }
            if (!removedAny) break; //the oldest remaining is inside the window
        }
    }

}


