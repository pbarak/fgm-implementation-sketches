package fgm.worker.processor.window.localwatermarks;

import org.apache.kafka.streams.processor.TaskId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class LocalWatermarks {
   
   private static final java.util.function.BiFunction<Long, Long, Long> MAX = Math::max;

    private LocalWatermarks() {}

    // TaskId -> max event time seen (ms)
    private static final ConcurrentHashMap<TaskId, Long> wm = new ConcurrentHashMap<>();
    // TaskId -> last wall-clock time we updated this entry (ms)
    private static final ConcurrentHashMap<TaskId, Long> lastSeen = new ConcurrentHashMap<>();

    public static void update(TaskId taskId, long eventTsMs, long nowMs) {
        wm.merge(taskId, eventTsMs, MAX);
        lastSeen.put(taskId, nowMs);
    }

    public static void remove(TaskId taskId) {
        wm.remove(taskId);
        lastSeen.remove(taskId);
    }

    /** Min watermark across tasks; optionally ignore tasks idle longer than idleTimeoutMs. */
    public static long minAcrossTasks(long idleTimeoutMs, long nowMs) {
        long min = Long.MAX_VALUE;
        for (Map.Entry<TaskId, Long> e : wm.entrySet()) {
            TaskId t = e.getKey();
            Long last = lastSeen.get(t);
            if (idleTimeoutMs > 0 && last != null && nowMs - last > idleTimeoutMs) {
                // long-idle task doesn’t hold back (policy)
                continue;
            }
            long w = e.getValue();
            if (w < min) min = w;
        }
        return (min == Long.MAX_VALUE) ? Long.MIN_VALUE : min;
    }

}
