package fgm.worker.processor.cashregister;

import fgm.common.cfg.BaseConfiguration;
import fgm.common.messages.StatisticsMessage;
import fgm.factory.specs.IFgmFactory;
import fgm.common.messages.IncrementMessage;
import fgm.worker.messagesender.IMessageSender;
import fgm.worker.metrictypes.StatisticsMessageType;
import fgm.worker.metrictypes.WorkerMetrics;
import fgm.worker.remoteobject.implementation.FgmNode;
import fgm.streamdata.StreamRecord;

import fgm.util.genericsutil.ComputePhiInvocationHelper;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

public class WorkerProcessor implements Processor<StreamRecord, Double> {

    private static final Logger logger = LoggerFactory.getLogger(WorkerProcessor.class);

    static final int SINGLE_PARTITION = 1;
    boolean iAmLeader;
    int srcTopicPartitionsNum;
    private static final AtomicReference<TaskId> JVM_LEADER = new AtomicReference<>();

    //generic client
    private FgmNode<?> fgmNode;
    private BaseConfiguration baseConfiguration;
    private IMessageSender messageSender;
    private boolean sentFirstDriftMessage = false;
    private static final String CSV_SEPARATOR = ",";
    private ProcessorContext processorContext;

    public WorkerProcessor(String id, IFgmFactory<?> fgmFactory, int srcTopicPartitionsNum) {
        try {
            this.srcTopicPartitionsNum = srcTopicPartitionsNum;
            this.baseConfiguration = fgmFactory.getBaseCfg();
            this.fgmNode = fgmFactory.getFgmNode();
            this.fgmNode.setNodeId(id);
            this.messageSender = fgmFactory.getMessageSender();
        } catch (Exception e) {
            logger.info(e.toString());
        }
    }


    //initialization of the processor, this function is called by Kafka Streams
    @Override
    public void init(final ProcessorContext processorContext) {
        this.processorContext = processorContext;
        if (srcTopicPartitionsNum > SINGLE_PARTITION) {
            // Leader = the task that owns partition 0
            final int myPartition = processorContext.partition();
            this.iAmLeader = (myPartition == 0);
        }
    }

    @Override
    public void process(StreamRecord key, Double value) {
        //here put warm up policy is optional
        this.checkIncrement(key, value);
    }

    private void checkIncrement(StreamRecord streamRecord, Double frequency) {
        if (!sentFirstDriftMessage) {
            try {
                fgmNode.getNodeInternalFgmState().getSi().update(streamRecord, frequency);
                if (iAmLeader) fgmNode.sendFirstDriftMessage();

                sendStateUpdateStatistics(streamRecord);

                sentFirstDriftMessage = true;
            } catch (Exception e) {
                logger.info(e.toString());
            }
        } else {
            try {
                if (fgmNode.getTheta() == 0) {
                    System.out.println("sleep");
                    Thread.sleep(1000);
                } else {
                    fgmNode.update(streamRecord, frequency, null);
                    sendUpdateLogMessage();

                    sendStateUpdateStatistics(streamRecord);

                    if (iAmLeader) {
                        //compute the safe function after the update of the drift vector
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
                            sendLogMessage();
                        }
                    }
                }
            } catch (Exception ex) {
                logger.info(ex.toString());
            }
        }
    }

    private void sendUpdateLogMessage() {
        //////String logMessage = this.fgmNode.getNodeId() + CSV_SEPARATOR + DateTime.now().getMillis() + CSV_SEPARATOR + WorkerMetrics.UPDATE;
        //////this.messageSender.sendMessageToSink(
        //////        this.processorContext,
        //////        this.fgmNode.getNodeId(),
        //////        logMessage,
        //////        baseConfiguration.getWorkerLogsUpdatesSinkName());
    }

    private void sendLogMessage() {
        String logMessage = fgmNode.getNodeId() + CSV_SEPARATOR + DateTime.now().getMillis() + CSV_SEPARATOR + WorkerMetrics.SENT_INCREMENT;
        this.messageSender.sendMessageToSink(
                this.processorContext,
                this.fgmNode.getNodeId(),
                logMessage,
                baseConfiguration.getWorkerLogsSinkName()
        );    }

    private void sendIncrementToCoordinator(int incrementCounter) {
        IncrementMessage incrementMessage = IncrementMessage.getInstance();
        incrementMessage.setSiteId(fgmNode.getNodeId());
        incrementMessage.setCurrentRound(fgmNode.getCurrentRound());
        incrementMessage.setCurrentSubround(fgmNode.getCurrentSubRound());
        incrementMessage.setIncrementCounter(incrementCounter);
        this.messageSender.sendMessageToSink(
                this.processorContext,
                baseConfiguration.getIncrementTopicKey(),
                incrementMessage,
                baseConfiguration.getWorkerIncrementSinkName()
        );
    }

    private boolean checkIfLocalCounterIsIncreased(int ci, double zi, double theta, double update) {
        return ci < (int) Math.floor((update - zi) / theta);
    }

    private StatisticsMessage getStatisticsMessage(StreamRecord streamRecord) {
        return StatisticsMessage.getInstance(
                fgmNode.getNodeInternalFgmState().getCurrentRound(),
                fgmNode.getNodeInternalFgmState().getCurrentSubRound(),
                fgmNode.getNodeInternalFgmState().getNodeId(),
                StatisticsMessageType.StreamRecordForStatistics.toString(),
                streamRecord);
    }

    private double getUpdateOfSafeFunction() {
        return ComputePhiInvocationHelper.invokeComputePhi(fgmNode);
    }


    private void sendStateUpdateStatistics(StreamRecord streamRecord) {
        String stateUpdateStatisticsKey = baseConfiguration.getStateUpdateStatisticsKey();
        String workerStatisticsSinkName = baseConfiguration.getWorkerStatisticEventsSinkName();

        this.messageSender.sendMessageToSink(
                this.processorContext,
                stateUpdateStatisticsKey,
                getStatisticsMessage(streamRecord),
                workerStatisticsSinkName
        );
    }

    @Override
    public void close() {
        logger.info("WorkerProcessor is shutting down.");
    }
}



