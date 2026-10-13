package fgm.worker.remoteobject.implementation;

import fgm.common.cfg.BaseConfiguration;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.*;

import fgm.messageproducer.IMessageProducer;
import fgm.worker.commands.implementation.SendPhiMessageCommand;
import fgm.worker.commands.implementation.SendDriftMessageCommand;
import fgm.worker.commands.implementation.StartNewRoundCommand;
import fgm.worker.commands.implementation.StartNewSubroundCommand;
import fgm.common.messages.DriftVectorMessage;
import fgm.common.messages.PhiMessage;
import fgm.common.GlobalStatistic;
import fgm.streamdata.StreamRecord;
import fgm.factory.specs.IFgmFactory;
import fgm.worker.commands.FgmCommand;
import fgm.queryfunction.QueryFunction;
import fgm.safefunction.SafeFunction;
import fgm.worker.metrictypes.WorkerMetrics;
import fgm.worker.remoteobject.NodeRMICollectionInterface;
import fgm.worker.statistics.LocalStatistic;
import fgm.worker.remoteobject.NodeRMIInterface;
import fgm.util.genericsutil.ComputePhiInvocationHelper;
import fgm.utils.SerializationUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import fgm.common.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

public class FgmNode<IMPL extends Vector> extends UnicastRemoteObject implements NodeRMIInterface<IMPL> {
    private static final Logger logger = LoggerFactory.getLogger("fgm.worker.remoteobject.implementation.FgmNode.metrics");

    private static final Logger UPDATES = LoggerFactory.getLogger("fgm.worker.remoteobject.implementation.FgmNode.updates");

    private final transient FgmNodeStatistics<IMPL> nodeInternalFgmState;
    private final transient BaseConfiguration baseConfiguration;
    private static final String CSV_SEPARATOR = ",";

    // Replaces 'synchronized' with Fair Locking for FIFO strictness
    private final transient ReentrantLock fairLock = new ReentrantLock(true);

    public FgmNode(IFgmFactory<IMPL> factory, FgmNodeStatistics<IMPL> sharedState) throws RemoteException {
        this.nodeInternalFgmState = sharedState;
        this.baseConfiguration = factory.getBaseCfg();

        try {
            Registry registry = LocateRegistry.getRegistry(baseConfiguration.getRmiHostName(), baseConfiguration.getRemoteStorePort());
            NodeRMICollectionInterface stub = (NodeRMICollectionInterface) registry.lookup(baseConfiguration.getRemoteStoreBindName());
            stub.add(this);
        } catch (Exception e) {
            logger.error("RMI Registration failed: {}", e.getMessage());
        }
    }

    private void sendSafeLog(WorkerMetrics metric, String topic, String messageSize) {
        try {
            String logMessage = this.nodeInternalFgmState.getNodeId() + CSV_SEPARATOR +
                                System.currentTimeMillis() + CSV_SEPARATOR + metric.name() + CSV_SEPARATOR + (messageSize == null ? "NULL" : messageSize);
	    logger.info(logMessage);
        } catch (Exception e) {
            logger.warn("Log dropped to protect critical path: {}", e.getMessage());
        }
    }

    private void sendUpdateLogToTopic(WorkerMetrics metric, String topic, String messageSize) {
        try {
            String logMessage = System.currentTimeMillis() + CSV_SEPARATOR +
                    this.nodeInternalFgmState.getNodeId()  + CSV_SEPARATOR +
                    metric.name() + CSV_SEPARATOR +
                    (messageSize == null ? "NULL" : messageSize);
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic,
                    this.nodeInternalFgmState.getNodeId(),
                    logMessage);
            logger.info(logMessage);
        } catch (Exception e) {
            logger.warn("Log dropped to protect critical path: {}", e.getMessage());
        }
    }

    @Override
    public void initializeFirstRound(GlobalStatistic<IMPL> globalEstimate, double theta) throws RemoteException {
        fairLock.lock();
        try {
            this.nodeInternalFgmState.setE(SerializationUtil.deepCopy(globalEstimate));
            this.nodeInternalFgmState.setTheta(theta);
	    
            this.nodeInternalFgmState.setZi(ComputePhiInvocationHelper.invokeComputePhi(this));
            this.nodeInternalFgmState.setCi(0);
            this.nodeInternalFgmState.setCurrentSubRound(1);
        } finally {
            fairLock.unlock();
        }
    }


/**
 * Pre-protocol warm-up entry point used by the windowed worker. Stages the
 * record into the window's slide buffer through the same path that the
 * regular protocol update uses, so that warm-up records will, after a
 * subsequent slidePunctuate, be deposited into the window's bucket queue
 * and the local sketch atomically.
 * Unlike {@link #update(StreamRecord, double, Long)}, this method does not
 * emit a metric log to the worker log topic, because warm-up records are
 * not part of the protocol's monitored stream. The maxTimeStamp is advanced
 * so that stream-time punctuation logic sees correct event-time progress
 * once the protocol takes over.
 *
 * The processor must call {@link #warmUpSlideFlush(long)} at slide boundaries
 * and once more at the warm-up boundary to commit the staged records.
 */
public void warmUpStateVector(StreamRecord streamRecord, double frequency, Long timestamp) {
    fairLock.lock();
    try {
        if (streamRecord == null) return;
        if (this.nodeInternalFgmState.getWindow() != null) {
            this.nodeInternalFgmState.getWindow().update(streamRecord, frequency, timestamp);
            if (timestamp != null) {
                this.nodeInternalFgmState.setMaxTimeStamp(timestamp);
            }
        } else {
            // Cash-register variant: no window, write the sketch directly.
            this.nodeInternalFgmState.getSi().update(streamRecord, frequency);
        }
    } finally {
        fairLock.unlock();
    }
}

/**
 * Drives a manual slide-punctuate on the window during warm-up. Drains the
 * window's slide buffer into the bucket queue and updates the local sketch,
 * which is the same sequence the protocol's STREAM_TIME punctuator performs
 * once the protocol is running.
 *
 * Has no effect in the cash-register (windowless) configuration; the sketch
 * has already been updated record-by-record inside warmUpStateVector.
 *
 * The processor calls this at every slide-sized event-time interval during
 * warm-up, and exactly once more at the warm-up boundary, so that all staged
 * records are committed before the first drift message is sent.
 */
public void warmUpSlideFlush(long eventTimeMs) {
    fairLock.lock();
    try {
        if (this.nodeInternalFgmState.getWindow() != null) {
            this.nodeInternalFgmState.getWindow().slidePunctuate(eventTimeMs);
        }
    } finally {
        fairLock.unlock();
    }
}


    public void update(StreamRecord streamRecord, double frequency, long timestamp) {
        fairLock.lock();
        try {
            if (streamRecord != null) {
                ////sendUpdateLogToTopic(WorkerMetrics.UPDATE, baseConfiguration.getWorkerLogsUpdatesTopic(), null);
                String logMessage = System.currentTimeMillis() + CSV_SEPARATOR +
                        streamRecord.getTimeStamp() + CSV_SEPARATOR +
                        this.nodeInternalFgmState.getNodeId()  + CSV_SEPARATOR +
                        WorkerMetrics.UPDATE + CSV_SEPARATOR + "NULL";
                UPDATES.info(logMessage);
                if (this.nodeInternalFgmState.getWindow() != null) {
                    this.nodeInternalFgmState.getWindow().update(streamRecord, frequency, timestamp);
                    this.nodeInternalFgmState.setMaxTimeStamp(timestamp);
                } else {
                    this.nodeInternalFgmState.getSi().update(streamRecord, frequency);
                }
            }
        } finally {
            fairLock.unlock();
        }
    }

    @Override
    public void startNewRound(GlobalStatistic<IMPL> globalEstimate, double theta) throws RemoteException {
        long payloadBytes = SerializationUtil.bytesOf(globalEstimate) + Double.BYTES; // thread-local arg, no lock needed
        fairLock.lock();
        try {
            sendSafeLog(WorkerMetrics.RECEIVED_ESTIMATE_AND_QUANTUM, baseConfiguration.getWorkerLogTopic(), Long.toString(payloadBytes));
            executeCommand(new StartNewRoundCommand(theta, globalEstimate, this));
        } finally {
            fairLock.unlock();
        }
    }

    @Override
    public void startNewSubround(double theta) throws RemoteException {
        fairLock.lock();
        try {
            int messageSize = Double.BYTES;
            String finalMessageSize = Integer.toString(messageSize);
            sendSafeLog(WorkerMetrics.RECEIVED_QUANTUM, baseConfiguration.getWorkerLogTopic(), finalMessageSize);
            executeCommand(new StartNewSubroundCommand(theta, this));
        } catch (Exception e) {
            logger.error(e.toString());
        } finally {
            fairLock.unlock();
        }
    }

    @Override
    public void subroundEnded() throws RemoteException {
        final SendPhiMessageCommand cmd = new SendPhiMessageCommand(this);
        fairLock.lock();
        try {
            sendSafeLog(WorkerMetrics.RECEIVED_REQ_PHI_MESSAGE, baseConfiguration.getWorkerLogTopic(), null);
            cmd.snapshotUnderLock();
        } finally {
            fairLock.unlock();
        }
        executeCommand(cmd);   // send + SENT_PHI log, off the lock
    }

    @Override
    public void roundEnded() throws RemoteException {
        final SendDriftMessageCommand sendDriftMessageCommand = new SendDriftMessageCommand(this);
        fairLock.lock();
        try {
            sendSafeLog(WorkerMetrics.RECEIVED_REQ_DRIFT, baseConfiguration.getWorkerLogTopic(), null);
            sendDriftMessageCommand.snapshotUnderLock();
        } finally {
            fairLock.unlock();
        }
        executeCommand(sendDriftMessageCommand);
    }

    public void sendFirstDriftMessage() {
        if (!this.nodeInternalFgmState.tryMarkFirstDriftSent()) return;

        DriftVectorMessage stateMessage = new DriftVectorMessage();
        stateMessage.setSiteId(getNodeId());
        
        fairLock.lock();
        try {
            stateMessage.setStateVector(getDrift().getVector());
            stateMessage.setCurrentRound(1);
            stateMessage.setCurrentSubround(0);
        } finally {
            fairLock.unlock();
        }

        try {
            long messageSize = SerializationUtil.bytesOf(stateMessage);
            String messageSizeString = Long.toString(messageSize);
            sendSafeLog(WorkerMetrics.SENT_DRIFT, baseConfiguration.getWorkerLogTopic(), messageSizeString);
            this.nodeInternalFgmState.getDriftMessagesProducer().send(
                    new ProducerRecord<>(baseConfiguration.getDriftMessagesTopic(), 
                    baseConfiguration.getDriftMessagesTopicKey(), stateMessage));
            this.nodeInternalFgmState.getDriftMessagesProducer().flush();
        } catch (Exception ex) {
            logger.error("Failed to send first drift: {}", ex.getMessage());
        }
    }

    public boolean checkIfLocalCounterIsIncreased(int ci, double zi, double theta, double update) {
        fairLock.lock();
        try {
            boolean increased = ci < (int) Math.floor((update - zi) / theta);
            
            ////if (increased) sendSafeLog(WorkerMetrics.SENT_INCREMENT, baseConfiguration.getWorkerLogTopic());

            return increased;
        } finally {
            fairLock.unlock();
        }
    }

    public void deleteRMINode() {
        try {
            Registry registry = LocateRegistry.getRegistry(baseConfiguration.getRmiHostName(), baseConfiguration.getRemoteStorePort());
            NodeRMICollectionInterface stub = (NodeRMICollectionInterface) registry.lookup(baseConfiguration.getRemoteStoreBindName());
            stub.remove(this);
        } catch (Exception e) {
            logger.error("Error unregistering node: {}", e.toString());
        }
    }


    public void logSentIncrement(long payloadBytes) {
    	fairLock.lock();
    	try {
        	sendSafeLog(WorkerMetrics.SENT_INCREMENT, baseConfiguration.getWorkerLogTopic(), Long.toString(payloadBytes));
    	} finally {
        	fairLock.unlock();
    	}
    }



/**
 * Records a SENT_PHI_MESSAGE event with the measured payload size, routed
 * through the same sendSafeLog path as every other worker protocol event.
 */
public void logSentPhi(long payloadBytes) {
    fairLock.lock();
    try {
        sendSafeLog(WorkerMetrics.SENT_PHI_MESSAGE,
                    baseConfiguration.getWorkerLogTopic(),
                    Long.toString(payloadBytes));
    } finally {
        fairLock.unlock();
    }
}

/**
 * Records a SENT_DRIFT event with the measured payload size, routed
 * through the same sendSafeLog path as every other worker protocol event.
 */
public void logSentDrift(long payloadBytes) {
    fairLock.lock();
    try {
        sendSafeLog(WorkerMetrics.SENT_DRIFT,
                    baseConfiguration.getWorkerLogTopic(),
                    Long.toString(payloadBytes));
    } finally {
        fairLock.unlock();
    }
}




    @Override
    public boolean ping() throws RemoteException { return true; }

    private void executeCommand(FgmCommand fgmCommand) { fgmCommand.execute(); }

    // --- LOCK-PROTECTED STATE ACCESSORS ---

    public int getCurrentRound() { fairLock.lock(); try { return this.nodeInternalFgmState.getCurrentRound(); } finally { fairLock.unlock(); } }
    
    public int getCurrentSubRound() { fairLock.lock(); try { return this.nodeInternalFgmState.getCurrentSubRound(); } finally { fairLock.unlock(); } }

    public QueryFunction<IMPL> getQueryFunction() { fairLock.lock(); try { return this.nodeInternalFgmState.getQueryFunction(); } finally { fairLock.unlock(); } }
    
    public double getT() { fairLock.lock(); try { return this.nodeInternalFgmState.getQueryFunction().computeQuery(this.nodeInternalFgmState.getE()); } finally { fairLock.unlock(); } }
    
    public LocalStatistic<IMPL> getDrift() { fairLock.lock(); try { return this.nodeInternalFgmState.getDrift(); } finally { fairLock.unlock(); } }
    
    public GlobalStatistic<IMPL> getE() { fairLock.lock(); try { return this.nodeInternalFgmState.getE(); } finally { fairLock.unlock(); } }
    
    public <K extends Vector> void setE(GlobalStatistic<K> globalEstimate) { fairLock.lock(); try { this.nodeInternalFgmState.setE(globalEstimate); } finally { fairLock.unlock(); } }
    
    public String getNodeId() { fairLock.lock(); try { return this.nodeInternalFgmState.getNodeId(); } finally { fairLock.unlock(); } }
    
    public void setNodeId(String id) { fairLock.lock(); try { this.nodeInternalFgmState.setNodeId(id); } finally { fairLock.unlock(); } }
    
    public double getTheta() { fairLock.lock(); try { return this.nodeInternalFgmState.getTheta(); } finally { fairLock.unlock(); } }
    
    public int getCi() { fairLock.lock(); try { return this.nodeInternalFgmState.getCi(); } finally { fairLock.unlock(); } }
    
    public void setCi(int ci) { fairLock.lock(); try { this.nodeInternalFgmState.setCi(ci); } finally { fairLock.unlock(); } }
    
    public double getZi() { fairLock.lock(); try { return this.nodeInternalFgmState.getZi(); } finally { fairLock.unlock(); } }
    
    public SafeFunction<IMPL> getSafeFunction() { fairLock.lock(); try { return this.nodeInternalFgmState.getSafeFunction(); } finally { fairLock.unlock(); } }
    
    public boolean isThetaEqualsZero() { fairLock.lock(); try { return this.nodeInternalFgmState.isThetaEqualsZero(); } finally { fairLock.unlock(); } }
    
    public void setThetaEqualsZero(boolean thetaEqualsZero) { fairLock.lock(); try { this.nodeInternalFgmState.setThetaEqualsZero(thetaEqualsZero); } finally { fairLock.unlock(); } }

    public Long getMaxTimeStamp() { fairLock.lock(); try { return this.nodeInternalFgmState.getMaxTimeStamp(); } finally { fairLock.unlock(); } }
    
    public void setLastMaxTimeStampPunctuation(Long timeStamp) { fairLock.lock(); try { this.nodeInternalFgmState.setLastTimeStampPunctuation(timeStamp); } finally { fairLock.unlock(); } }

    public boolean streamTimePunctuation() {
        fairLock.lock();
        try {
            long diff = this.nodeInternalFgmState.getMaxTimeStamp() - this.nodeInternalFgmState.getLastTimeStampPunctuation();
            if (diff >= baseConfiguration.getSlideSizeMillis()) {
                setLastMaxTimeStampPunctuation(this.nodeInternalFgmState.getMaxTimeStamp());
                return true;
            }
            return false;
        } finally {
            fairLock.unlock();
        }
    }

    // Direct access to producers (StateStore handles their internal safety)
    public IMessageProducer<String, DriftVectorMessage> getDriftMessagesProducer() { return this.nodeInternalFgmState.getDriftMessagesProducer(); }
    public FgmNodeStatistics<IMPL> getNodeInternalFgmState() { return this.nodeInternalFgmState; }
}