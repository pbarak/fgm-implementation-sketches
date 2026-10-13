package fgm.worker.processor.window.localwatermarks;

import org.apache.kafka.streams.processor.TaskId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class LocalWatermarks {

    private LocalWatermarks() {}

    /** Per-task mutable holder: max event time seen + last wall-clock update (ms). */
    private static final class Mark {
        volatile long wm = Long.MIN_VALUE;
        volatile long seen;
    }

    // TaskId -> watermark state
    private static final ConcurrentHashMap<TaskId, Mark> marks = new ConcurrentHashMap<>();

    public static void update(TaskId taskId, long eventTsMs, long nowMs) {
        Mark m = marks.computeIfAbsent(taskId, t -> new Mark());
        if (eventTsMs > m.wm) m.wm = eventTsMs; // single-writer per task: plain read-then-write is safe
        m.seen = nowMs;
    }

    public static void remove(TaskId taskId) {
        marks.remove(taskId);
    }

    /** Min watermark across tasks; optionally ignore tasks idle longer than idleTimeoutMs. */
    public static long minAcrossTasks(long idleTimeoutMs, long nowMs) {
        long min = Long.MAX_VALUE;
        for (Map.Entry<TaskId, Mark> e : marks.entrySet()) {
            Mark m = e.getValue();
            long w = m.wm;
            if (w == Long.MIN_VALUE) {
                // entry created but no event recorded yet
                continue;
            }
            if (idleTimeoutMs > 0 && nowMs - m.seen > idleTimeoutMs) {
                // long-idle task doesn’t hold back (policy)
                continue;
            }
            if (w < min) min = w;
        }
        return (min == Long.MAX_VALUE) ? Long.MIN_VALUE : min;
    }

}