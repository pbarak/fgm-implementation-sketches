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
    private static final Logger logger = LoggerFactory.getLogger(SendDriftMessageCommand.class);
    private static final String CSV_SEPARATOR = ",";

    public SendDriftMessageCommand() {
    }

    public SendDriftMessageCommand(FgmNode<?> fgmNode) {
        this.fgmNode = fgmNode;
        this.siteId = this.fgmNode.getNodeId();
    }

    @Override
    public void execute() {
        try {
            getStatisticsProducer().flush();

            sendDrift();

            sendStatisticsMessage();

            sendLogMessage();

        } catch (Exception ex) {
            logger.info(ex.toString());
        }
    }

    private void sendDrift() {
        DriftVectorMessage stateMessage = DriftVectorMessage.getInstance();
        stateMessage.setSiteId(this.siteId);
        stateMessage.setStateVector(this.fgmNode.getDrift().getVector());
        stateMessage.setCurrentRound(this.fgmNode.getCurrentRound());
        stateMessage.setCurrentSubround(this.fgmNode.getCurrentSubRound());


        // Measure the payload before sending so the size goes into the
        // communication-cost log with the SENT_DRIFT event below.
        this.lastSentBytes = SerializationUtil.bytesOf(stateMessage);


        String driftMessageTopic = getBaseConfiguration().getDriftMessagesTopic();
        String driftMessageKey = getBaseConfiguration().getDriftMessagesTopicKey();
        ProducerRecord<String, DriftVectorMessage> producerRecord = new ProducerRecord<>(driftMessageTopic, driftMessageKey, stateMessage);
        getDriftMessageProducer().send(producerRecord);
    }

    private IMessageProducer<String, DriftVectorMessage> getDriftMessageProducer() {
        return this.fgmNode.getDriftMessagesProducer();
    }

    private void sendStatisticsMessage() {
        String statisticsTopic = getBaseConfiguration().getStatisticEventsTopic();
        String statisticsKey = getBaseConfiguration().getStateUpdateStatisticsKey();
        Integer currentRound = this.fgmNode.getCurrentRound();
        Integer currentSubround = this.fgmNode.getCurrentSubRound();
        String nodeId = this.fgmNode.getNodeId();
        StatisticsMessage statisticsMessage = StatisticsMessage.getInstance(currentRound, currentSubround, nodeId, StatisticsMessageType.DriftMessage.toString());
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

    private IMessageProducer<String, String> getLogsProducer() {
        return this.fgmNode.getNodeInternalFgmState().getLogsProducer();
    }
}



