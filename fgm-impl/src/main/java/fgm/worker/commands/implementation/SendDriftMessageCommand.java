package fgm.worker.commands.implementation;

import fgm.common.cfg.BaseConfiguration;
import fgm.common.messages.StatisticsMessage;
import fgm.common.messages.DriftVectorMessage;
import fgm.messageproducer.IMessageProducer;
import fgm.worker.commands.FgmCommand;
import fgm.worker.metrictypes.StatisticsMessageType;
import fgm.worker.metrictypes.WorkerMetrics;
import fgm.worker.remoteobject.implementation.FgmNode;
import fgm.utils.SerializationUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SendDriftMessageCommand implements FgmCommand {
    private String siteId;
    private FgmNode<?> fgmNode;
    private long lastSentBytes;     // payload size of the most recent send, for logging

    private DriftVectorMessage driftMessage;
    private StatisticsMessage statisticsMessage;

    private static final Logger logger = LoggerFactory.getLogger(SendDriftMessageCommand.class);

    public SendDriftMessageCommand() {
    }

    public SendDriftMessageCommand(FgmNode<?> fgmNode) {
        this.fgmNode = fgmNode;
    }

    /**
     * Phase 1 — MUST be invoked while holding the node's fairLock.
     * Reads all protocol state exactly once and builds both outbound
     * messages. getDrift() returns an independent copy of Si - Ei
     * (FastAGMS.subtract allocates a fresh sketch and setCounts deep-copies),
     * so after this method returns the messages share no state with the
     * live sketches.
     */
    public void snapshotUnderLock() {
        String siteId = this.fgmNode.getNodeId();
        int currentRound = this.fgmNode.getCurrentRound();
        int currentSubround = this.fgmNode.getCurrentSubRound();

        driftMessage = DriftVectorMessage.getInstance();
        driftMessage.setSiteId(siteId);
        driftMessage.setStateVector(this.fgmNode.getDrift().getVector());
        driftMessage.setCurrentRound(currentRound);
        driftMessage.setCurrentSubround(currentSubround);

        statisticsMessage = StatisticsMessage.getInstance(
                currentRound, currentSubround, siteId,
                StatisticsMessageType.DriftMessage.toString());
    }


    @Override
    public void execute() {
        try {

            sendDrift();

            sendStatisticsMessage();

            sendLogMessage();

        } catch (Exception ex) {
            logger.info(ex.toString());
        }
    }

    private void sendDrift() {
            // Measure the payload before sending so the size goes into the
            // communication-cost log with the SENT_DRIFT event below.
            this.lastSentBytes = SerializationUtil.bytesOf(driftMessage);

            String driftMessageTopic = getBaseConfiguration().getDriftMessagesTopic();
            String driftMessageKey = getBaseConfiguration().getDriftMessagesTopicKey();
            ProducerRecord<String, DriftVectorMessage> producerRecord = new ProducerRecord<>(driftMessageTopic, driftMessageKey, driftMessage);
            getDriftMessageProducer().send(producerRecord);

    }

    private IMessageProducer<String, DriftVectorMessage> getDriftMessageProducer() {
        return this.fgmNode.getDriftMessagesProducer();
    }

    private void sendStatisticsMessage() {
        String statisticsTopic = getBaseConfiguration().getStatisticEventsTopic();
        String statisticsKey = getBaseConfiguration().getStateUpdateStatisticsKey();
        ProducerRecord<String, StatisticsMessage> statisticsRecord = new ProducerRecord<>(statisticsTopic, statisticsKey, statisticsMessage);
        getStatisticsProducer().send(statisticsRecord);
    }

    private IMessageProducer<String, StatisticsMessage> getStatisticsProducer() {
        return this.fgmNode.getNodeInternalFgmState().getStatisticsMessageProducer();
    }

    private void sendLogMessage() {
        // Routes through FgmNode so the line shares appender + format with every
        // other worker protocol event (nodeId,timestamp,event,sizeBytes).
        fgmNode.logSentDrift(this.lastSentBytes);
    }

    private BaseConfiguration getBaseConfiguration() {
        return this.fgmNode.getNodeInternalFgmState().getBaseConfiguration();
    }
}



