package fgm.streamdata.implementation.stream.window;

import fgm.streamdata.StreamRecord;

import java.util.Objects;

/**
 * Per-record entry held inside the sliding window.
 * todo
 * GC notes:
 *  - Internal fields are primitives (double / long / long). One {@code StreamWindowRecord}
 *    is allocated per stream record arriving on the inbound topic; eliminating the three
 *    boxed fields removes three additional heap objects per record.
 *  - The interface contract on {@link StreamRecord#getTimeStamp()} returns {@code Long},
 *    so the override has to auto-box on each call. Callers in hot paths (the slide
 *    punctuator, the eviction loop, comparators) should use the primitive accessors
 *    {@link #getMillisecondsTimestamp()}, {@link #getSecondsTimestamp()} and
 *    {@link #getFrequencyValue()} to avoid that auto-boxing.
 *  - {@link #equals} compares timestamps as primitives. The key comparison still goes
 *    through {@link StreamRecord#getKey()} which returns {@code Long}; eliminating that
 *    boxing requires adding a primitive accessor to the {@link StreamRecord} interface.
 */
public class StreamWindowRecord implements StreamRecord {

    private static final long serialVersionUID = 1L;

    private StreamRecord record;
    private double frequency;
    private long secondsPreciseTimestamp;
    private long millisecondsPreciseTimestamp;

    /** No-arg constructor retained for Jackson / framework deserialization. */
    public StreamWindowRecord() {
        // primitive defaults: 0.0, 0L, 0L
    }

    public StreamWindowRecord(StreamRecord streamRecord, double frequency,
                              long secondsPreciseTimestamp, long millisecondsPreciseTimestamp) {
        this.record = streamRecord;
        this.frequency = frequency;
        this.secondsPreciseTimestamp = secondsPreciseTimestamp;
        this.millisecondsPreciseTimestamp = millisecondsPreciseTimestamp;
    }

    /**
     * Boxed-argument constructor retained for backward compatibility with callers
     * that still hold {@code Double} / {@code Long}. Null arguments default to zero.
     */
    public StreamWindowRecord(StreamRecord streamRecord, Double frequency,
                              Long secondsPreciseTimestamp, Long millisecondsPreciseTimestamp) {
        this.record = streamRecord;
        this.frequency = (frequency != null) ? frequency : 0.0d;
        this.secondsPreciseTimestamp = (secondsPreciseTimestamp != null) ? secondsPreciseTimestamp : 0L;
        this.millisecondsPreciseTimestamp = (millisecondsPreciseTimestamp != null) ? millisecondsPreciseTimestamp : 0L;
    }


    /** Milliseconds-precise timestamp as a primitive {@code long}. No allocation. */
    public long getMillisecondsTimestamp() {
        return millisecondsPreciseTimestamp;
    }

    /** Seconds-precise timestamp as a primitive {@code long}. No allocation. */
    public long getSecondsTimestamp() {
        return secondsPreciseTimestamp;
    }

    /** Frequency as a primitive {@code double}. No allocation. */
    public double getFrequencyValue() {
        return frequency;
    }

    public void setMillisecondsTimestamp(long timestamp) {
        this.millisecondsPreciseTimestamp = timestamp;
    }

    public void setSecondsTimestamp(long timestamp) {
        this.secondsPreciseTimestamp = timestamp;
    }

    public void setFrequencyValue(double frequency) {
        this.frequency = frequency;
    }


    /** {@inheritDoc} Allocates a {@code Long} per call. Prefer {@link #getMillisecondsTimestamp()}. */
    @Override
    public Long getTimeStamp() {
        return millisecondsPreciseTimestamp;
    }

    /** Boxed setter retained for compatibility. Null is treated as a no-op. */
    public void setTimeStamp(Long timestamp) {
        if (timestamp != null) {
            this.millisecondsPreciseTimestamp = timestamp;
        }
    }

    /** Allocates a {@code Long} per call. Prefer {@link #getSecondsTimestamp()}. */
    public Long getSecondsPreciseTimestamp() {
        return secondsPreciseTimestamp;
    }

    /** Boxed setter retained for compatibility. */
    public void setSecondsPreciseTimestamp(Long timestamp) {
        if (timestamp != null) {
            this.secondsPreciseTimestamp = timestamp;
        }
    }

    /** Allocates a {@code Double} per call. Prefer {@link #getFrequencyValue()}. */
    public Double getFrequency() {
        return frequency;
    }

    /** Boxed setter retained for compatibility. */
    public void setFrequency(Double frequency) {
        if (frequency != null) {
            this.frequency = frequency;
        }
    }

    public StreamRecord getRecord() {
        return record;
    }

    public void setRecord(StreamRecord record) {
        this.record = record;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Long getKey() {
        return record.getKey();
    }

    // ---------------------------------------------------------------------
    // equals / hashCode
    //
    // Timestamp comparison is fully primitive (no boxing, no .longValue() calls).
    // The key comparison still goes through StreamRecord.getKey() which returns
    // Long; this is unavoidable without changing the StreamRecord interface.
    // ---------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StreamWindowRecord that = (StreamWindowRecord) o;

        if (this.millisecondsPreciseTimestamp != that.millisecondsPreciseTimestamp) {
            return false;
        }

        Object thisKey = (this.record == null) ? null : this.record.getKey();
        Object thatKey = (that.record == null) ? null : that.record.getKey();
        return Objects.equals(thisKey, thatKey);
    }

    @Override
    public int hashCode() {
        int h = Long.hashCode(millisecondsPreciseTimestamp);
        Object key = (record == null) ? null : record.getKey();
        h = 31 * h + (key == null ? 0 : key.hashCode());
        return h;
    }
}