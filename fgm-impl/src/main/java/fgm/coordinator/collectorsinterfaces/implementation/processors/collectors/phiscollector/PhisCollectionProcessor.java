package fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.phiscollector;

import fgm.common.GlobalStatistic;
import fgm.common.Vector;
import fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.CoordinatorMetrics;
import fgm.queryfunction.QueryFunction;
import fgm.coordinator.collectorsinterfaces.implementation.processors.statestore.CoordinatorStateStore;
import fgm.coordinator.collectorsinterfaces.implementation.processors.state.FgmCoordinatorState;
import fgm.common.messages.PhiMessage;
import fgm.coordinator.collectorsinterfaces.PhisCollectorInterface;
import fgm.utils.SerializationUtil;
import fgm.worker.messagesender.IMessageSender;
import fgm.worker.remoteobject.NodeRMICollectionInterface;
import fgm.worker.remoteobject.NodeRMIInterface;
import fgm.util.genericsutil.ComputePhiInvocationHelper;
import fgm.coordinator.utils.RMINetworkDispatcher;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.Naming;

public class PhisCollectionProcessor implements Processor<String, PhiMessage>, PhisCollectorInterface {

    private static final String CSV_SEPARATOR = ",";
    private static final String ACTOR = "coordinator";
    // For state-machine markers that don't correspond to a transmission.
    private static final String NO_SIZE = "NULL";

    private CoordinatorStateStore<?> coordinatorStateStore;
    private NodeRMICollectionInterface remoteNodeStore;
    private final String stateStoreName;
    private final QueryFunction<?> queryFunction;
    private FgmCoordinatorState<?> fgmCoordinatorState;
    private ProcessorContext processorContext;
    private final IMessageSender messageSender;
    private static final String PHIS_LOG_APPENDER = "fgm.coordinator.trace.Phis";

    private static final Logger logger = LoggerFactory.getLogger(PhisCollectionProcessor.class);
    private static final Logger trace = LoggerFactory.getLogger(PHIS_LOG_APPENDER);

    public PhisCollectionProcessor(String stateStoreName, String rmiServerUrl, IMessageSender messageSender,
                                   QueryFunction<?> queryFunction) {
        this.stateStoreName = stateStoreName;
        this.messageSender = messageSender;
        this.queryFunction = queryFunction;
        try {
            remoteNodeStore = (NodeRMICollectionInterface) Naming.lookup(rmiServerUrl);
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }

    @Override
    public void init(final ProcessorContext context) {
        processorContext = context;
        coordinatorStateStore = (CoordinatorStateStore<?>) context.getStateStore(stateStoreName);
        fgmCoordinatorState = coordinatorStateStore.getFgmCoordinatorState();
    }

    @Override
    public void process(String key, PhiMessage value) {
        this.collectPhis(value);
    }

    public void collectPhis(PhiMessage phiMessage) {
        collectMsgAndNotifyNodes(phiMessage);
    }

    void collectMsgAndNotifyNodes(PhiMessage phiMessage) {
        logPhiMessageReceivedInformation(phiMessage);
        // Each phi arrival is an inbound communication event: measure and log.
        logReceivedPhiMessage(phiMessage);

        int numberOfNodes = fgmCoordinatorState.getNumberOfNodes();
        if (phiMessage.getCurrentRound() == fgmCoordinatorState.getCurrentRound()
                && phiMessage.getCurrentSubround() == fgmCoordinatorState.getCurrentSubround() - 1) {
            fgmCoordinatorState.getPhis().add(phiMessage);
            if (fgmCoordinatorState.getPhis().size() == numberOfNodes) {
                fgmCoordinatorState.setPsi(fgmCoordinatorState.getPhis().calculatePhi());
                logThetaChangedInformation();
                double queryResult = fgmCoordinatorState.computeQuery();
                if (fgmCoordinatorState.getPsi() >= (fgmCoordinatorState.getEPsi() * numberOfNodes *
                        fgmCoordinatorState.computePhiWithZeroDrift(queryResult))) {
                    fgmCoordinatorState.setCurrentRound(fgmCoordinatorState.getCurrentRound() + 1);
                    fgmCoordinatorState.setCurrentSubround(0);
                    notifyNodesForRoundEnded();
                } else {
                    fgmCoordinatorState.setTheta((((-1) * fgmCoordinatorState.getPsi()) /
                            (Integer.valueOf(2 * numberOfNodes)).doubleValue()));
                    notifyNodesForNewSubround(fgmCoordinatorState.getTheta());
                }
            }
        }
    }

    public void notifyNodesForNewSubround(double theta) {
        try {
            java.util.List<NodeRMIInterface<?>> allNodes = remoteNodeStore.getAll();
            // startNewSubround(double theta): payload is one double, 8 bytes.
            final int rmiPayloadBytes = Double.BYTES;

            for (int i = 0; i < allNodes.size(); i++) {
                final NodeRMIInterface<?> node = allNodes.get(i);
                
                logSubroundDispatch(rmiPayloadBytes);

                RMINetworkDispatcher.dispatch(node, () -> {
                    try {
                        node.startNewSubround(theta);
                    } catch (Exception e) {
                        logger.error("Error in startNewSubround RMI call: {}", e.getMessage());
                    }
                });
            }
            logNewSubround();   // state-machine marker, no size
        } catch (Exception e) {
            logger.info(e.toString());
        }
    }

    public void notifyNodesForRoundEnded() {
        try {
            java.util.List<NodeRMIInterface<?>> allNodes = remoteNodeStore.getAll();
            // roundEnded(): no arguments, 0 bytes.
            final long rmiPayloadBytes = 0L;

            for (int i = 0; i < allNodes.size(); i++) {
                final NodeRMIInterface<?> node = allNodes.get(i);

                logSentReqDrift(rmiPayloadBytes);

                RMINetworkDispatcher.dispatch(node, () -> {
                    try {
                        node.roundEnded();
                    } catch (Exception e) {
                        logger.error("Error in roundEnded RMI call: {}", e.getMessage());
                    }
                });
            }
        } catch (Exception e) {
            logger.info(e.toString());
        }
    }

    private void logPhiMessageReceivedInformation(PhiMessage phiMessage) {
        logger.info(
                "the phiMessage.getCurrentRound() is {} this.currentRound {} phiMessage.getCurrentSubround() {} this.currentSubround {} this.phi is {} the site id is {}",
                phiMessage.getCurrentRound(),
                fgmCoordinatorState.getCurrentRound(),
                phiMessage.getCurrentSubround(),
                fgmCoordinatorState.getCurrentSubround(),
                phiMessage.getPhiValue(),
                phiMessage.getSiteId()
        );
    }

    private void logThetaChangedInformation() {
        logger.info("the new psi is {}", fgmCoordinatorState.getPsi());
        logger.info("I CHANGED THETA {}", fgmCoordinatorState.getTheta());
        double queryResult = fgmCoordinatorState.computeQuery();
        logger.info(
                "this.ePsi*this.k*(calculateSafeFunction.zeroArgumentResult(this.E)) is {}",
                fgmCoordinatorState.getEPsi()
                        * fgmCoordinatorState.getNumberOfNodes()
                        * fgmCoordinatorState.computePhiWithZeroDrift(queryResult));
    }

    // ----- communication-cost log lines -----
    // Shape: actor,timestamp,event,target,sizeBytes

    private void logSubroundDispatch(long sizeBytes) {
        trace.info(ACTOR + CSV_SEPARATOR
                + DateTime.now().getMillis() + CSV_SEPARATOR
                + CoordinatorMetrics.SENT_REQ_PHI_MESSAGE
                + CSV_SEPARATOR
                + sizeBytes);
    }

    private void logNewSubround() {
        // State-machine marker, not a transmission. Size = NULL.
        trace.info(ACTOR + CSV_SEPARATOR
                + DateTime.now().getMillis() + CSV_SEPARATOR
                + CoordinatorMetrics.SUBROUND
                + CSV_SEPARATOR
                + NO_SIZE);
    }

    private void logSentReqDrift(long sizeBytes) {
        trace.info(ACTOR + CSV_SEPARATOR
                + DateTime.now().getMillis() + CSV_SEPARATOR
                + CoordinatorMetrics.SENT_REQ_DRIFT
                + CSV_SEPARATOR
                + sizeBytes);
    }

    private void logReceivedPhiMessage(PhiMessage phi) {
        long bytes = SerializationUtil.bytesOf(phi);
        trace.info(ACTOR + CSV_SEPARATOR
                + DateTime.now().getMillis() + CSV_SEPARATOR
                + CoordinatorMetrics.RECEIVED_PHI_MESSAGE
                + CSV_SEPARATOR     // who sent it to us
                + bytes);
    }

    @Override
    public void close() {
        logger.info("PhisCollectionProcessor is shutting down.");
    }
}