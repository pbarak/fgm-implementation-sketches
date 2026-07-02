package fgm.worker.processor.window;

import fgm.common.cfg.BaseConfiguration;
import fgm.common.messages.DriftVectorMessage;
import fgm.common.messages.StatisticsMessage;
import fgm.worker.messagesender.IMessageSender;
import fgm.worker.metrictypes.StatisticsMessageType;
import fgm.worker.remoteobject.implementation.FgmNode;
import fgm.streamdata.StreamRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import fgm.worker.processor.window.localwatermarks.LocalWatermarks;
import org.apache.kafka.streams.processor.PunctuationType;

public class WorkerWindowProcessor extends WorkerWindowProcessorBase implements Processor<StreamRecord, Double>  {

    private static final Logger logger = LoggerFactory.getLogger(WorkerWindowProcessor.class);

    public WorkerWindowProcessor(String id, BaseConfiguration baseConfiguration, IMessageSender messageSender, FgmNode<?> fgmNode, int srcTopicPartitionsNum) {
        super(id, baseConfiguration, messageSender,  fgmNode, srcTopicPartitionsNum);
    }


    //initialization of the processor, this function is called by Kafka Streams
    @Override
    public void init(final ProcessorContext processorContext) {
        this.processorContext = processorContext;
        if (srcTopicPartitionsNum > SINGLE_PARTITION) {
            // Leader = the task that owns partition 0
            final int myPartition = processorContext.taskId().partition;
            this.iAmLeader = (myPartition == 0);

            if (iAmLeader) {
                processorContext.schedule(
                        baseConfiguration.getMultiPartitionPunctuationMillis(),
                        baseConfiguration.getMultiPartitionPunctuationType(),        // time domain
                        multiPartitionPunctuatorWithWaterMark
                );
            }
        } else {
            // single thread processing, no need for leadership election
            processorContext.schedule(
                    baseConfiguration.getSlideSizeMillis(),
                    baseConfiguration.getPunctuationType(),      // time domain todo
                    singlePartitionPunctuator
            );
        }
    }

    @Override
    public void process(StreamRecord key, Double value) {
        // 1) update JVM-local watermark
        long eventTs = this.processorContext.timestamp(); // event time chosen by your TimestampExtractor
        long now     = System.currentTimeMillis();
        LocalWatermarks.update(processorContext.taskId(), eventTs, now);
        if(value == -1.0d) {
            logger.info("ENTERED IN sending first Drift message {}", this.fgmNode.getNodeInternalFgmState().getNodeId());
            DriftVectorMessage stateMessage = new DriftVectorMessage();
            stateMessage.setSiteId(this.fgmNode.getNodeInternalFgmState().getNodeId());
            stateMessage.setStateVector(this.fgmNode.getNodeInternalFgmState().getDrift().getVector());
            stateMessage.setCurrentRound(this.fgmNode.getNodeInternalFgmState().getCurrentRound()+1);
            stateMessage.setCurrentSubround(0);

            try {
                this.fgmNode.getNodeInternalFgmState().getDriftMessagesProducer().send(
                        new ProducerRecord<>(baseConfiguration.getDriftMessagesTopic(), baseConfiguration.getDriftMessagesTopicKey(), stateMessage));
            } catch (Exception ex) {
                logger.info(ex.getMessage());
            }



            //////String statisticsTopic = baseConfiguration.getStatisticEventsTopic();
            //////String statisticsKey = baseConfiguration.getDriftMessagesTopicKey();
            //////Integer currentRound = this.fgmNode.getCurrentRound();
            //////Integer currentSubround = this.fgmNode.getCurrentSubRound();
            //////String nodeId = this.fgmNode.getNodeId();
            //////StatisticsMessage statisticsMessage = StatisticsMessage.getInstance(currentRound, currentSubround, nodeId, StatisticsMessageType.DriftMessage.toString());
            //////ProducerRecord<String, StatisticsMessage> statisticsRecord = new ProducerRecord<>(statisticsTopic, statisticsKey, statisticsMessage);
            //////this.fgmNode.getNodeInternalFgmState().getStatisticsMessageProducer().send(statisticsRecord);
        } else {
            this.updateStateVector(key, value);
        }
    }



    @Override
    public void close() {
        if (processorContext != null) {
            LocalWatermarks.remove(processorContext.taskId());
            this.fgmNode.deleteRMINode();
        }
    }

}


