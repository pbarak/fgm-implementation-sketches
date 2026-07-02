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
            // subroundEnded(): no arguments, 0 bytes.
            final long rmiPayloadBytes = 0L;

            for (int i = 0; i < allNodes.size(); i++) {
                final NodeRMIInterface<?> node = allNodes.get(i);

                logSentRequestPhiMessage(rmiPayloadBytes);

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
        logIncrementReceived(incrementMessage);
        collectMsgAndNotifyNodes(incrementMessage);
    }

    void collectMsgAndNotifyNodes(IncrementMessage incrementMessage) {
        synchronized (RMINetworkDispatcher.GLOBAL_PHASE_LOCK) {
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
    }

    // ----- communication-cost log lines -----
    // Shape: actor,timestamp,event,target,sizeBytes

    private void logIncrementReceived(IncrementMessage inc) {
        long bytes = SerializationUtil.bytesOf(inc);
        trace.info(ACTOR + CSV_SEPARATOR
                + DateTime.now().getMillis() + CSV_SEPARATOR
                + CoordinatorMetrics.RECEIVED_INCREMENT 
		+ CSV_SEPARATOR
                + bytes);
    }

    private void logSentRequestPhiMessage(long sizeBytes) {
        trace.info(ACTOR + CSV_SEPARATOR
                + DateTime.now().getMillis() + CSV_SEPARATOR
                + CoordinatorMetrics.SENT_REQ_PHI_MESSAGE
                + CSV_SEPARATOR
                + sizeBytes);
    }

    @Override
    public void close() {
        logger.info("IncrementCollectionProcessor is shutting down.");
    }
}