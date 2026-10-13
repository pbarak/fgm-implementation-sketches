package fgm.worker.processor.window;

import fgm.common.Vector;
import fgm.common.cfg.BaseConfiguration;
import fgm.common.messages.IncrementMessage;
import fgm.common.messages.StatisticsMessage;
import fgm.streamdata.StreamRecord;
import fgm.util.genericsutil.ComputePhiInvocationHelper;
import fgm.utils.SerializationUtil;
import fgm.worker.messagesender.IMessageSender;
import fgm.worker.metrictypes.StatisticsMessageType;
import fgm.worker.metrictypes.WorkerMetrics;
import fgm.worker.remoteobject.implementation.FgmNode;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import fgm.worker.processor.window.localwatermarks.LocalWatermarks;

public class WorkerWindowProcessorBase {

    private static final Logger logger = LoggerFactory.getLogger(WorkerWindowProcessorBase.class);

    static final int SINGLE_PARTITION = 1;
    boolean iAmLeader;
    int srcTopicPartitionsNum;
    //generic client
    FgmNode<?> fgmNode;
    BaseConfiguration baseConfiguration;
    IMessageSender messageSender;
    boolean sentFirstDriftMessage = false;

    // --- WARM-UP STATE ---
    // Event-time timestamp of the very first record observed by this processor instance.
    // Remains null until the first record arrives. Used to measure elapsed event-time
    // against the configured window width (window.width seconds in env.properties).
    private Long warmUpStartTimestamp = null;
    // Flips to true once at least window.width seconds of event-time have elapsed since
    // the first record. From that point on, no further calls into warmUpStateVector occur.
    private boolean warmedUp = false;
    // When false (warmup.enabled=false, or any multi-partition run), the accumulation phase is
    // skipped: the first drift message is emitted on the first record so the protocol (and the
    // watermark punctuator) starts immediately instead of waiting windowWidth/numberOfNodes ms.
    // Defaults to false so that if the constructor fails to read the config it fails safe
    // (warm-up disabled), which also guarantees the warm-up branch only runs single-partition.
    private boolean warmUpEnabled = false;

    static final String CSV_SEPARATOR = ",";
    ProcessorContext processorContext;

    public WorkerWindowProcessorBase(String id,
                                     BaseConfiguration baseConfiguration,
                                     IMessageSender messageSender,
                                     FgmNode<?> fgmNode,
                                     int srcTopicPartitionsNum) {
        try {
            this.baseConfiguration = baseConfiguration;
            this.fgmNode = fgmNode;
            this.fgmNode.setNodeId(id);
            this.messageSender = messageSender;
            this.srcTopicPartitionsNum = srcTopicPartitionsNum;
            // Warm-up is always disabled in the multi-partition case: the node state is a shared
            // singleton across partitions, so per-partition warm-up gating is incoherent and can
            // stall the protocol (the leader's partition-0 event-time may never reach the boundary).
            // In single-partition mode it still follows the warmup.enabled config flag.
            this.warmUpEnabled = baseConfiguration.isWarmUpEnabled()
                    && srcTopicPartitionsNum <= SINGLE_PARTITION;
        } catch (Exception e) {
            logger.info(e.toString());
        }
    }


private Long lastWarmUpSlideEventTime = null;   // last event-time we flushed at

// In WorkerWindowProcessorBase
void warmUpStateVector(StreamRecord streamRecord, Double frequency) {
    if (streamRecord == null) return;

    long now = streamRecord.getTimeStamp();
    if (warmUpStartTimestamp == null) {
        warmUpStartTimestamp = now;
        lastWarmUpSlideEventTime = now;
    }

    // Stage into the window via the FgmNode facade (lock-protected).
    fgmNode.warmUpStateVector(streamRecord, frequency, now);

    // Drive the slide punctuation manually on event-time slide intervals.
    long slideMs = baseConfiguration.getSlideSizeMillis();
    if (now - lastWarmUpSlideEventTime >= slideMs) {
        fgmNode.warmUpSlideFlush(now);
        lastWarmUpSlideEventTime = now;
    }

    long elapsedMs = now - warmUpStartTimestamp;
    long warmUpDurationMs =
    ((long) baseConfiguration.getWindowSecondsWidth() * 1000L)
        / baseConfiguration.getNumberOfNodes();
    if (elapsedMs >= warmUpDurationMs) {
        // Final flush to commit any remaining staged records.
        fgmNode.warmUpSlideFlush(now);
        warmedUp = true;
    }
}

    void updateStateVector(StreamRecord streamRecord, Double frequency) {
        // Phase 1: warm-up. The state vector is populated for window.width seconds of event-time
        // before any drift message is emitted. The record that completes warm-up is the same
        // record that triggers the first drift message  and no record is dropped at the boundary.
        if (!warmedUp) {
            if (!warmUpEnabled) {
                // Warm-up disabled: skip accumulation. Emit the first drift message once so the
                // coordinator sets theta and the protocol (and watermark punctuator) starts now.
                // Then fall through to Phase 2 so this same record is processed normally.
                if (!sentFirstDriftMessage) {
                    try {
                        if (srcTopicPartitionsNum > SINGLE_PARTITION) {
                            if (iAmLeader) {
				fgmNode.getNodeInternalFgmState().getSi().update(streamRecord, frequency);
                                fgmNode.sendFirstDriftMessage();
                                LocalWatermarks.remove(processorContext.taskId());
                            }
                        } else {
			                fgmNode.getNodeInternalFgmState().getSi().update(streamRecord, frequency);
                            fgmNode.sendFirstDriftMessage();
                        }
                        sentFirstDriftMessage = true;
                    } catch (Exception e) {
                        logger.info(e.toString());
                    }
                }
                warmedUp = true;
                // intentionally no return: continue to Phase 2 below for this record
            } else {
                try {
                    warmUpStateVector(streamRecord, frequency);
                } catch (Exception e) {
                    logger.info(e.toString());
                    return;
                }

                if (!warmedUp) {
                    // Still accumulating, nothing else to do for this record.
                    return;
                }

                // Warm-up just completed on this record. Emit the first drift message, using the
                // already-warmed state vector. The record itself has already been accumulated above
                // by warmUpStateVector, so we must NOT also call fgmNode.update() for it.
                // Reachable only in single-partition mode (warm-up is force-disabled whenever
                // srcTopicPartitionsNum > SINGLE_PARTITION), so no leader gating is needed here.
                try {
                    fgmNode.sendFirstDriftMessage();
                    sentFirstDriftMessage = true;
                } catch (Exception e) {
                    logger.info(e.toString());
                }
                return;
            }
        }

        // Phase 2: regular protocol. theta is set by the coordinator once it has heard from all
        // sites; until then we briefly pause rather than spin.
        try {
            if (fgmNode.getTheta() == 0) {
                System.out.println("sleep");
                Thread.sleep(1000);
            } else {
                fgmNode.setThetaEqualsZero(false);
                fgmNode.update(streamRecord, frequency, streamRecord.getTimeStamp());
                //////sendStateUpdateStatistics(streamRecord);
            }
        } catch (Exception ex) {
            logger.info(ex.toString());
        }
    }


    private void sendStateUpdateStatistics(StreamRecord streamRecord) {
        //String stateUpdateKey = baseConfiguration.getStateUpdateStatisticsKey();
        //long now = System.currentTimeMillis();
        //Record<String, Object> messageRecord = new Record<>(stateUpdateKey, getStateUpdateStatisticsMessage(streamRecord), now);
        //String workerStatisticsSinkName = baseConfiguration.getWorkerStatisticEventsSinkName();

        String statisticsTopic = baseConfiguration.getStatisticEventsTopic();
        String stateUpdateKey = baseConfiguration.getStateUpdateStatisticsKey();
        ProducerRecord<String, StatisticsMessage> messageRecord = new ProducerRecord<>(statisticsTopic,null,getStateUpdateStatisticsMessage(streamRecord));
        this.fgmNode.getNodeInternalFgmState().getStatisticsMessageProducer().send(messageRecord);


        //this.fgmNode.getNodeInternalFgmState().getStatisticsMessageProducer().flush();
        //this.messageSender.sendMessageToSink(this.processorContext, messageRecord, workerStatisticsSinkName);
    }

     void sendIncrementToCoordinator(int incrementCounter) {
        IncrementMessage incrementMessage = IncrementMessage.getInstance();
        incrementMessage.setSiteId(fgmNode.getNodeId());
        incrementMessage.setCurrentRound(fgmNode.getCurrentRound());
        incrementMessage.setCurrentSubround(fgmNode.getCurrentSubRound());
        incrementMessage.setIncrementCounter(incrementCounter);

        long bytes = SerializationUtil.bytesOf(incrementMessage);          

         this.messageSender.sendMessageToSink(
                 this.processorContext,
                 baseConfiguration.getIncrementTopicKey(),
                 incrementMessage,
                 baseConfiguration.getWorkerIncrementSinkName()
         );
         fgmNode.logSentIncrement(bytes);
    }

     boolean checkIfLocalCounterIsIncreased(int ci, double zi, double theta, double update) {
        return ci < (int) Math.floor((update - zi) / theta);
    }

     StatisticsMessage getStateUpdateStatisticsMessage(StreamRecord streamRecord) {
        return StatisticsMessage.getInstance(
                fgmNode.getNodeInternalFgmState().getCurrentRound(),
                fgmNode.getNodeInternalFgmState().getCurrentSubRound(),
                fgmNode.getNodeInternalFgmState().getNodeId(),
                StatisticsMessageType.StreamRecordForStatistics.toString(),
                streamRecord);
    }

     double getUpdateOfSafeFunction() {
        return ComputePhiInvocationHelper.invokeComputePhi(fgmNode);
    }


     final Punctuator multiPartitionPunctuator = timestamp -> {
        if (!fgmNode.isThetaEqualsZero()){// && fgmNode.streamTimePunctuation()) {
            //compute the safe function after the update of the drift vector
            fgmNode.getNodeInternalFgmState().getWindow().slidePunctuate(this.processorContext.timestamp());
            double updateOfSafeFunction = getUpdateOfSafeFunction();

            //if local counter is increased send increase of C message
            if (checkIfLocalCounterIsIncreased(
                    fgmNode.getCi(),
                    fgmNode.getZi(),
                    fgmNode.getTheta(), updateOfSafeFunction)) {

                int incrementCounter = (int) Math.floor(
                        (updateOfSafeFunction - fgmNode.getZi())
                                / fgmNode.getTheta())
                        - fgmNode.getCi();

                //set the Ci after the increase according to paper
                fgmNode.setCi((int) Math.floor(
                        (updateOfSafeFunction - fgmNode.getZi())
                                / fgmNode.getTheta()));

                sendIncrementToCoordinator(incrementCounter);
            }
        }
    };

    final Punctuator singlePartitionPunctuator = streamTime -> {
        if (!fgmNode.isThetaEqualsZero()) {
            //compute the safe function after the update of the drift vector
            fgmNode.getNodeInternalFgmState().getWindow().slidePunctuate(streamTime);
            double updateOfSafeFunction = getUpdateOfSafeFunction();

            //if local counter is increased send increase of C message
            if (checkIfLocalCounterIsIncreased(
                    fgmNode.getCi(),
                    fgmNode.getZi(),
                    fgmNode.getTheta(), updateOfSafeFunction)) {

                int incrementCounter = (int) Math.floor(
                        (updateOfSafeFunction - fgmNode.getZi())
                                / fgmNode.getTheta())
                        - fgmNode.getCi();

                //set the Ci after the increase according to paper
                fgmNode.setCi((int) Math.floor(
                        (updateOfSafeFunction - fgmNode.getZi())
                                / fgmNode.getTheta()));

                sendIncrementToCoordinator(incrementCounter);
            }
        }
    };


    // Safety cap so a single tick can't do unbounded work
    private static final int MAX_STEPS_PER_TICK = 32;

    final Punctuator multiPartitionPunctuatorWithWaterMark = ts -> {
        if (fgmNode.isThetaEqualsZero()) return; // still warming up

        final long now = System.currentTimeMillis();

        // IMPORTANT: ignore idle tasks (like partition 0 leader that stops receiving data)
        // If you don't have a cfg, pick something sane, e.g., a few slide widths
        final long idleTimeoutMs = Math.max(baseConfiguration.getSlideSizeMillis() * 4, 2_000L);

        final long wm = LocalWatermarks.minAcrossTasks(idleTimeoutMs, now);
        if (wm == Long.MIN_VALUE) return; // nothing has arrived anywhere yet

        final long slide  = baseConfiguration.getSlideSizeMillis();
        final long grace  = baseConfiguration.getSlideSizeMillis(); // or a cfg for lateness tolerance

        // Snap watermark down to the slide grid
        long effective = Math.max(0L, wm - grace);
        final long targetBoundary = (effective / slide) * slide;

        // Get last advanced boundary (event-time), ensure it's on-grid
        final java.util.concurrent.atomic.AtomicLong last = fgmNode.getNodeInternalFgmState().lastAdvancedBoundary();

        // One-time init: put "last" on grid just behind the target so the first step lands on target
        for (;;) {
            long prev = last.get();
            if (prev == Long.MIN_VALUE) {
                long init = Math.max(0L, targetBoundary - slide); // start one step behind target
                if (last.compareAndSet(prev, init)) break;
            } else {
                // sanitize if somehow off-grid
                long snappedPrev = (prev / slide) * slide;
                if (snappedPrev == prev || last.compareAndSet(prev, snappedPrev)) break;
            }
        }

        int steps = 0;
        while (steps < MAX_STEPS_PER_TICK) {
            long cur = last.get();
            if (cur >= targetBoundary) break;     // up to date w.r.t. watermark grid

            long next = cur + slide;              // EXACT multiple of slide
            if (!last.compareAndSet(cur, next)) continue;

            // Advance your window to the new slide boundary
            fgmNode.getNodeInternalFgmState().getWindow().slidePunctuate(next);

            // Recompute and emit increments idempotently
            double update = getUpdateOfSafeFunction();
            int desiredCi = (int) Math.floor((update - fgmNode.getZi()) / fgmNode.getTheta());
            int inc = desiredCi - fgmNode.getCi();
            if (inc > 0) {
                fgmNode.setCi(desiredCi);
                sendIncrementToCoordinator(inc);
            }
            steps++;
        }
    };

}
