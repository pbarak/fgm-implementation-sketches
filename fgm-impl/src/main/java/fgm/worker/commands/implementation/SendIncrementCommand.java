//package fgm.worker.commands.implementation;
//
//import fgm.common.MessageType;
//import fgm.common.cfg.BaseConfiguration;
//import fgm.common.messages.StatisticsMessage;
//import fgm.common.messages.IncrementMessage;
//import fgm.messageproducer.IMessageProducer;
//import fgm.worker.commands.FgmCommand;
//import fgm.worker.remoteobject.implementation.FgmNode;
//import fgm.streamdata.StreamRecord;
//import fgm.util.genericsutil.ComputePhiInvocationHelper;
//import org.apache.kafka.clients.producer.Producer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
////the SendIncrementCommand is now executed in the implementation of the worker processor
//@Deprecated
//public class SendIncrementCommand implements FgmCommand {
//    private static final Logger logger = LoggerFactory.getLogger(SendIncrementCommand.class);
//    static final long serialVersionUID = 1L;
//    private StreamRecord record; //reference to the current record of the stream
//    private FgmNode<?> fgmNode; //the bean that implements the Node. Reference according to the Command Pattern
//    private int currentRound;
//    private int currentSubround;
//
//    public SendIncrementCommand(StreamRecord value, FgmNode<?> fgmNode) {
//        this.record = value;
//        this.fgmNode = fgmNode;
//        this.currentRound = this.fgmNode.getCurrentRound();
//        this.currentSubround = this.fgmNode.getCurrentSubRound();
//    }
//
//    @Override
//    public void execute() {
//        try {
//            //compute the safe function after the update of the drift vector
//            double updateOfSafeFunction = ComputePhiInvocationHelper.invokeComputePhi(this.fgmNode);
//
//            //if local counter is increased send increment message
//            if (this.fgmNode.checkIfLocalCounterIsIncreased(
//                    this.fgmNode.getCi(),
//                    this.fgmNode.getZi(),
//                    this.fgmNode.getTheta(), updateOfSafeFunction)) {
//
//                int incrementCounter = (int) Math.floor(
//                        (updateOfSafeFunction - this.fgmNode.getZi())
//                                / this.fgmNode.getTheta())
//                        - this.fgmNode.getCi();
//
//                //set the Ci after the increase according to paper
//                this.fgmNode.setCi((int) Math.floor(
//                        (updateOfSafeFunction - this.fgmNode.getZi())
//                                / this.fgmNode.getTheta()));
//
//                sendIncrementMessage(incrementCounter);
//
//                sendStatisticsMessage();
//            }
//        } catch (Exception ex) {
//            logger.info(ex.getMessage());
//        }
//
//    }
//
//    private void sendStatisticsMessage() {
//        String statisticsTopic = getBbaseConfiguration().getStatisticEventsTopic();
//        String statisticsTopicKey = getBbaseConfiguration().getIncrementTopicKey();
//        int currentRound = this.fgmNode.getCurrentRound();
//        int currentSubround = this.fgmNode.getCurrentSubRound();
//        String nodeId = this.fgmNode.getNodeId();
//        StatisticsMessage statisticsMessage = StatisticsMessage.getInstance(currentRound, currentSubround, nodeId, MessageType.IncrementMessage.toString());
//        ProducerRecord<String, StatisticsMessage> statisticsRecord = new ProducerRecord<>(statisticsTopic, statisticsTopicKey, statisticsMessage);
//        getStatisticsProducer().send(statisticsRecord);
//    }
//
//    private void sendIncrementMessage(int incrementCounter) {
//        IncrementMessage incrementMessage = IncrementMessage.getInstance();
//        incrementMessage.setSiteId(this.fgmNode.getNodeId());
//        incrementMessage.setCurrentRound(this.currentRound);
//        incrementMessage.setCurrentSubround(this.currentSubround);
//        incrementMessage.setIncrementCounter(incrementCounter);
//
//        String coordinatorTopic = getBbaseConfiguration().getIncrementTopic();
//        String coordinatorTopicKey = getBbaseConfiguration().getIncrementTopicKey();
//        ProducerRecord<String, IncrementMessage> incrementRecord = new ProducerRecord<>(coordinatorTopic, coordinatorTopicKey, incrementMessage);
//        getIncrementProducer().send(incrementRecord);
//    }
//
//    private BaseConfiguration getBbaseConfiguration() {
//        return this.fgmNode.getNodeInternalFgmState().getBaseConfiguration();
//    }
//
//    private IMessageProducer<String, StatisticsMessage> getStatisticsProducer() {
//        return this.fgmNode.getNodeInternalFgmState().getStatisticsMessageProducer();
//    }
//
//    private Producer<String, IncrementMessage> getIncrementProducer() {
//        return this.fgmNode.getNodeInternalFgmState().getIncrementProducer();
//    }
//}
//
//
//
//
