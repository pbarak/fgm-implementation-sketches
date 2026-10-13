package fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.driftcollector;

import fgm.common.Vector;
import fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.CoordinatorMetrics;
import fgm.coordinator.collectorsinterfaces.implementation.processors.driftcollection.model.DriftVectorEntry;
import fgm.coordinator.collectorsinterfaces.implementation.processors.statestore.CoordinatorStateStore;
import fgm.coordinator.collectorsinterfaces.implementation.processors.state.FgmCoordinatorState;
import fgm.common.messages.DriftVectorMessage;
import fgm.coordinator.collectorsinterfaces.DriftCollectorInterface;
import fgm.common.GlobalStatistic;
import fgm.queryfunction.QueryFunction;
import fgm.worker.messagesender.IMessageSender;
import fgm.worker.remoteobject.NodeRMIInterface;
import fgm.worker.remoteobject.NodeRMICollectionInterface;
import fgm.util.genericsutil.ComputePhiInvocationHelper;
import fgm.utils.SerializationUtil;
import fgm.coordinator.utils.RMINetworkDispatcher;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.Naming;
import java.rmi.RemoteException;

public class DriftCollectionProcessor implements Processor<String, DriftVectorMessage>, DriftCollectorInterface {

    private static final String CSV_SEPARATOR = ",";
    private static final String ACTOR = "coordinator";
    private static final String NULL_MESSAGE_SIZE = "NULL";

    private CoordinatorStateStore<?> coordinatorStateStore;
    private NodeRMICollectionInterface remoteStoreCollection;
    private final String stateStoreName;
    private FgmCoordinatorState<?> fgmCoordinatorState;
    private ProcessorContext processorContext;
    private final IMessageSender messageSender;
    private final QueryFunction<?> queryFunction;
    private static final String DRIFT_LOG_APPENDER = "fgm.coordinator.trace.Drift";

    private static final Double NEGATION_FACTOR = -1.0d;

    private static final Logger logger = LoggerFactory.getLogger(DriftCollectionProcessor.class);
    private static final Logger trace = LoggerFactory.getLogger(DRIFT_LOG_APPENDER);

    public DriftCollectionProcessor(String stateStoreName, String rmiServerUrl, IMessageSender messageSender,
                                    QueryFunction<?> queryFunction) {
        this.messageSender = messageSender;
        this.queryFunction = queryFunction;
        this.stateStoreName = stateStoreName;
        try {
            remoteStoreCollection = (NodeRMICollectionInterface) Naming.lookup(rmiServerUrl);
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
    public void process(String key, DriftVectorMessage value) {
        this.updateGlobalEstimate(value);
    }

    public void updateGlobalEstimate(DriftVectorMessage driftVectorMessage) {
        logReceivedDriftInformation(driftVectorMessage);
        // Inbound communication event: measure and log per-message (per worker).
        logReceivedDrift(driftVectorMessage);

        populateDriftCollection(driftVectorMessage);

        if (fgmCoordinatorState.getDriftCollection().getDrifts().size() == fgmCoordinatorState.getNumberOfNodes()) {
            synchronized (fgmCoordinatorState) {
                populateEisCollection(fgmCoordinatorState);
                createAndFlushEStatistic(fgmCoordinatorState);
            }
        }
    }

    private <T extends Vector> void populateEisCollection(FgmCoordinatorState<T> state) {
        int numberOfNodes = state.getNumberOfNodes();
        for (int i = 0; i < numberOfNodes; i++) {
            state.getDriftCollection().getEntry(i).getDriftNodeDriftVector().scale((double) numberOfNodes);
        }
        if (state.getEisCollection().getEis().isEmpty()) {
            for (int i = 0; i < numberOfNodes; i++) {
                state.getEisCollection().collectEi(state.getDriftCollection().getEntry(i));
            }
        } else {
            for (int i = 0; i < numberOfNodes; i++) {
                for (int k = 0; k < numberOfNodes; k++) {
                    if (state.getEisCollection().getEntry(i).getEiNodeId()
                            .equals(state.getDriftCollection().getEntry(k).getDriftNodeId())) {
                        T Xi = state.getDriftCollection().getEntry(k).getDriftNodeDriftVector();
                        state.getEisCollection().getEntry(i).getEiLocalStatistic().add(Xi);
                    }
                }
            }
        }
    }

    private <T extends Vector> void createAndFlushEStatistic(FgmCoordinatorState<T> state) {
        int numberOfNodes = state.getNumberOfNodes();
        GlobalStatistic<T> globalStatisticCopy = SerializationUtil.deepCopy(state.getE());

        globalStatisticCopy.getVector().scale(NEGATION_FACTOR);
        state.getE().getVector().add(globalStatisticCopy.getVector());

        for (int k = 0; k < numberOfNodes; k++) {
            state.getE().addVectors(state.getEisCollection().getEntry(k).getEiLocalStatistic());
        }
        state.getDriftCollection().getDrifts().clear();
        double queryResult = state.computeQuery();
        logger.info("THE RESULT IS {}", queryResult);
        try {
            state.setCurrentSubround(0);
            state.setPsi(numberOfNodes * state.computePhiWithZeroDrift(queryResult));
            state.setTheta((((-1) * state.getPsi()) /
                    (Integer.valueOf(2 * numberOfNodes)).doubleValue()));

            this.notifyNodesForNewRound();

            state.setStarted(true);
        } catch (Exception e) {
            logger.info(e.toString());
        }
    }

    public void notifyNodesForNewRound() {
        try {
            boolean isStarted = fgmCoordinatorState.getIfStarted();
            GlobalStatistic<?> currentE = fgmCoordinatorState.getE();
            double currentTheta = fgmCoordinatorState.getTheta();


            java.util.List<NodeRMIInterface<?>> allNodes = remoteStoreCollection.getAll();

            for (int i = 0; i < allNodes.size(); i++) {
                final NodeRMIInterface<?> stateMessageBean = allNodes.get(i);

                // Per-target dispatch log with measured payload size.
                logSentEstimate();

                RMINetworkDispatcher.dispatch(stateMessageBean, () -> {
                    try {
                        shipRound(stateMessageBean, currentE, currentTheta, !isStarted);
                    } catch (Exception e) {
                        logger.error("Error in startNewRound/initializeFirstRound: {}", e.toString());
                    }
                });
            }

            if (!isStarted) {
                fgmCoordinatorState.setCurrentSubround(1);
            }
            logNewRound();    // state-machine marker, no size
        } catch (Exception ex) {
            logger.info(ex.toString());
        }
    }

    private void populateDriftCollection(DriftVectorMessage stateMessage) {
        boolean found = false;
        if (!fgmCoordinatorState.getDriftCollection().getDrifts().isEmpty()) {
            for (DriftVectorEntry<?> drift : fgmCoordinatorState.getDriftCollection().getDrifts()) {
                if (drift.getDriftNodeId().equals(stateMessage.getSiteId())) {
                    found = true;
                }
            }
            if (!found) {
                fgmCoordinatorState.getDriftCollection().collectDrift(stateMessage);
            }
        } else {
            fgmCoordinatorState.getDriftCollection().collectDrift(stateMessage);
        }
    }

    private void logReceivedDriftInformation(DriftVectorMessage stateMessage) {
        logger.info(
                "the DriftMessage.getCurrentRound() is {} this.currentRound {} DriftMessage.getCurrentSubround() {} this.currentSubRound {} the site id is {}",
                stateMessage.getCurrentRound(),
                fgmCoordinatorState.getCurrentRound(),
                stateMessage.getCurrentSubround(),
                fgmCoordinatorState.getCurrentSubround(),
                stateMessage.getSiteId());
    }

    // ----- communication-cost log lines -----
    // Shape: actor,timestamp,event,sizeBytes

    private void logNewRound() {
        trace.info(ACTOR + CSV_SEPARATOR
                + DateTime.now().getMillis() + CSV_SEPARATOR
                + CoordinatorMetrics.ROUND
                + CSV_SEPARATOR
                + NULL_MESSAGE_SIZE);
    }

    private void logSentEstimate() {
        trace.info(ACTOR + CSV_SEPARATOR
                + DateTime.now().getMillis() + CSV_SEPARATOR
                + CoordinatorMetrics.SENT_ESTIMATE
                + CSV_SEPARATOR
                + NULL_MESSAGE_SIZE);
    }

    private void logReceivedDrift(DriftVectorMessage msg) {
        trace.info(ACTOR + CSV_SEPARATOR
                + DateTime.now().getMillis() + CSV_SEPARATOR
                + CoordinatorMetrics.RECEIVED_DRIFT
                + CSV_SEPARATOR
                + NULL_MESSAGE_SIZE);
    }

    @Override
    public void close() {
        logger.info("DriftCollectionProcessor is shutting down.");
    }

    private static <T extends Vector> void shipRound(
            NodeRMIInterface<T> node, GlobalStatistic<?> E, double theta, boolean firstRound)
            throws RemoteException {
        @SuppressWarnings("unchecked")                       // unavoidable RMI-boundary reconciliation:
        GlobalStatistic<T> typedE = (GlobalStatistic<T>) E;  // the token cannot cross the erased wire
        if (firstRound) node.initializeFirstRound(typedE, theta);
        else            node.startNewRound(typedE, theta);
    }
}