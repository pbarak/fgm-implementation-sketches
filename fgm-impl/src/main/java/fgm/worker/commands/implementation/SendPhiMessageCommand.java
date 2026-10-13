package fgm.worker.commands.implementation;

import fgm.common.cfg.BaseConfiguration;
import fgm.common.messages.StatisticsMessage;
import fgm.common.messages.PhiMessage;
import fgm.messageproducer.IMessageProducer;
import fgm.worker.commands.FgmCommand;
import fgm.worker.metrictypes.StatisticsMessageType;
import fgm.worker.remoteobject.implementation.FgmNode;
import fgm.common.GlobalStatistic;
import fgm.common.Vector;
import fgm.worker.statistics.LocalStatistic;
import fgm.util.genericsutil.ComputePhiInvocationHelper;
import fgm.utils.SerializationUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import fgm.worker.metrictypes.WorkerMetrics;

public class SendPhiMessageCommand implements FgmCommand {

    private PhiMessage phiMessage;
    private FgmNode<?> fgmNode;
    private long lastSentBytes;     // payload size of the most recent send, for logging
    private static final Logger logger = LoggerFactory.getLogger(SendPhiMessageCommand.class);
    private static final String CSV_SEPARATOR = ",";

    public SendPhiMessageCommand() {
    }

    public SendPhiMessageCommand(FgmNode<?> fgmNode) {
        this.fgmNode = fgmNode;
    }

    /** Phase 1 — MUST run while holding fgmNode's fairLock. */
    public void snapshotUnderLock() {
        phiMessage = PhiMessage.getInstance();
        phiMessage.setSiteId(this.fgmNode.getNodeId());
        phiMessage.setCurrentRound(this.fgmNode.getCurrentRound());
        phiMessage.setCurrentSubround(this.fgmNode.getCurrentSubRound());
        phiMessage.setPhi(ComputePhiInvocationHelper.invokeComputePhi(this.fgmNode)); // needs lock-consistent Si/Ei/E
    }

    @Override
    public void execute() {
        try {

            this.lastSentBytes = SerializationUtil.bytesOf(phiMessage);
            ProducerRecord<String, PhiMessage> record = new ProducerRecord<>(
                    getBaseConfiguration().getPhiMessagesTopic(),
                    getBaseConfiguration().getPhiMessagesTopicKey(), phiMessage);
            getPhiMessageProducer().send(record);
            fgmNode.logSentPhi(this.lastSentBytes);

        } catch (Exception ex) {
            logger.info(ex.getMessage());
        }
    }

    private void sendPhiMessage() {
        PhiMessage phiMessage = PhiMessage.getInstance();
        phiMessage.setSiteId(this.fgmNode.getNodeId());
        ////phiMessage.setCurrentRound(this.currentRound);
        ////phiMessage.setCurrentSubround(this.currentSubRound);
        logger.info("currently in SendPhiMessageCommand of {} requesting getDrift() at {}",
                fgmNode.getNodeId(), DateTime.now().getMillis());
        LocalStatistic<? extends Vector> localDrift = this.fgmNode.getDrift();
        GlobalStatistic<? extends Vector> localE = this.fgmNode.getE();

        Double result = ComputePhiInvocationHelper.invokeComputePhi(this.fgmNode);

        phiMessage.setPhi(result);

	// Measure the payload before sending so the size goes into the
        // communication-cost log with the SENT_PHI_MESSAGE event below.
        this.lastSentBytes = SerializationUtil.bytesOf(phiMessage);

        String phiMessageTopic = getBaseConfiguration().getPhiMessagesTopic();
        String phiMessageKey = getBaseConfiguration().getPhiMessagesTopicKey();
        ProducerRecord<String, PhiMessage> producedPhiMessage = new ProducerRecord<>(phiMessageTopic, phiMessageKey, phiMessage);
        getPhiMessageProducer().send(producedPhiMessage);
        //////getPhiMessageProducer().flush();
    }

//      statistic message for logging purposes
//    private void sendPhiStatisticsMessage() {
//        String nodeId = this.fgmNode.getNodeId();
//        StatisticsMessage statisticsMessage = StatisticsMessage.getInstance(currentRound, currentSubRound, nodeId, StatisticsMessageType.PhiMessage.toString());
//        String statisticsMessageTopic = getBaseConfiguration().getStatisticEventsTopic();
//        String phiMessageKey = getBaseConfiguration().getPhiMessagesTopicKey();
//        ProducerRecord<String, StatisticsMessage> producerRecord = new ProducerRecord<>(statisticsMessageTopic, phiMessageKey, statisticsMessage);
//
//        getStatisticsProducer().send(producerRecord);
//    }

    private void sendLogMessage() {
                // Routes through FgmNode so the line shares appender + format with every
        // other worker protocol event (nodeId,timestamp,event,sizeBytes).
        fgmNode.logSentPhi(this.lastSentBytes);
    }

    private IMessageProducer<String, StatisticsMessage> getStatisticsProducer() {
        return this.fgmNode.getNodeInternalFgmState().getStatisticsMessageProducer();
    }

    private IMessageProducer<String, PhiMessage> getPhiMessageProducer() {
        return this.fgmNode.getNodeInternalFgmState().getPhiMessagesProducer();
    }

    private BaseConfiguration getBaseConfiguration() {
        return this.fgmNode.getNodeInternalFgmState().getBaseConfiguration();
    }
}
