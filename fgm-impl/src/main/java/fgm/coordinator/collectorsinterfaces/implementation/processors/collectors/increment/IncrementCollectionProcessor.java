package fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.increment;

import fgm.common.messages.IncrementMessage;
import fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.CoordinatorMetrics;
import fgm.coordinator.collectorsinterfaces.implementation.processors.statestore.CoordinatorStateStore;
import fgm.coordinator.collectorsinterfaces.implementation.processors.state.FgmCoordinatorState;
import fgm.coordinator.collectorsinterfaces.IncrementsCollectorInterface;
import fgm.coordinator.utils.RMINetworkDispatcher;
import fgm.utils.SerializationUtil;
import fgm.worker.messagesender.IMessageSender;
import fgm.worker.remoteobject.NodeRMIInterface;
import fgm.worker.remoteobject.NodeRMICollectionInterface;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.Naming;

public class IncrementCollectionProcessor implements Processor<String, IncrementMessage>, IncrementsCollectorInterface {

    private static final String CSV_SEPARATOR = ",";
    private static final String ACTOR = "coordinator";

    private static final String NULL_MESSAGE_SIZE = "NULL";
    private ProcessorContext processorContext;
    private final String stateStoreName;
    private NodeRMICollectionInterface remoteNodeStore;
    private FgmCoordinatorState<?> fgmCoordinatorState;
    private IMessageSender messageSender;
    private static final String INCREMENTS_LOG_APPENDER = "fgm.coordinator.trace.Increments";

    private static final Logger logger = LoggerFactory.getLogger(IncrementCollectionProcessor.class);
    private static final Logger trace = LoggerFactory.getLogger(INCREMENTS_LOG_APPENDER);

    public IncrementCollectionProcessor(String stateStoreName, String rmiServerUrl, IMessageSender messageSender) {
        this.messageSender = messageSender;
        this.stateStoreName = stateStoreName;
        try {
            remoteNodeStore = (NodeRMICollectionInterface) Naming.lookup(rmiServerUrl);
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }

    @Override
    public void init(final ProcessorContext context) {
        processorContext = context;
        CoordinatorStateStore<?> coordinatorStateStore = (CoordinatorStateStore<?>) context.getStateStore(stateStoreName);
        fgmCoordinatorState = coordinatorStateStore.getFgmCoordinatorState();
    }

    @Override
    public void process(String key, IncrementMessage value) {
        this.collectIncrements(value);
    }

    private void logIncrementReceivedInformation(IncrementMessage increment) {
        logger.info(
                "the increment.getCurrentRound() is {} this.currentRound {} increment.getCurrentSubround() {} this.currentSubRound {}",
                increment.getCurrentRound(),
                fgmCoordinatorState.getCurrentRound(),
                increment.getCurrentSubround(),
                fgmCoordinatorState.getCurrentSubround());
    }

    public void notifyNodesForSubroundEnded() {
        try {
            java.util.List<NodeRMIInterface<?>> allNodes = remoteNodeStore.getAll();

            for (int i = 0; i < allNodes.size(); i++) {
                final NodeRMIInterface<?> node = allNodes.get(i);

                logSentRequestPhiMessage();

                RMINetworkDispatcher.dispatch(node, () -> {
                    try {
                        node.subroundEnded();
                    } catch (Exception e) {
                        logger.error("Error in collect Increment RMI call to site: {}", e.getMessage());
                    }
                });
            }
        } catch (Exception e) {
            logger.info("Error fetching nodes for RMI dispatch: {}", e.getMessage());
        }
    }

    public void collectIncrements(IncrementMessage incrementMessage) {
        logIncrementReceivedInformation(incrementMessage);
        // Inbound communication event: measure and log per-message.
        logIncrementReceived();
        collectMsgAndNotifyNodes(incrementMessage);
    }

    void collectMsgAndNotifyNodes(IncrementMessage incrementMessage) {
            if (incrementMessage.getCurrentRound() == fgmCoordinatorState.getCurrentRound()
                    && incrementMessage.getCurrentSubround() == fgmCoordinatorState.getCurrentSubround()) {

                fgmCoordinatorState.setGlobalC(fgmCoordinatorState.getGlobalC() + incrementMessage.getIncrementCounter());

                if (fgmCoordinatorState.getGlobalC() > fgmCoordinatorState.getNumberOfNodes()) {
                    fgmCoordinatorState.setGlobalC(0);
                    fgmCoordinatorState.setCurrentSubround(fgmCoordinatorState.getCurrentSubround() + 1);
                    notifyNodesForSubroundEnded();
                }
            }
    }

    // ----- communication-cost log lines -----
    // Shape: actor,timestamp,event,target,sizeBytes

    private void logIncrementReceived() {
        trace.info(ACTOR + CSV_SEPARATOR
                + DateTime.now().getMillis() + CSV_SEPARATOR
                + CoordinatorMetrics.RECEIVED_INCREMENT 
		        + CSV_SEPARATOR
                + NULL_MESSAGE_SIZE);
    }

    private void logSentRequestPhiMessage() {
        trace.info(ACTOR + CSV_SEPARATOR
                + DateTime.now().getMillis() + CSV_SEPARATOR
                + CoordinatorMetrics.SENT_REQ_PHI_MESSAGE
                + CSV_SEPARATOR
                + NULL_MESSAGE_SIZE);
    }

    @Override
    public void close() {
        logger.info("IncrementCollectionProcessor is shutting down.");
    }
}