package fgm.worker.remoteobject.implementation;

import fgm.common.cfg.BaseConfiguration;
import fgm.common.messages.DriftVectorMessage;
import fgm.common.messages.IncrementMessage;
import fgm.common.messages.PhiMessage;
import fgm.common.GlobalStatistic;
import fgm.common.Vector;
import fgm.common.messages.StatisticsMessage;
import fgm.factory.specs.IFgmFactory;
import fgm.messageproducer.IMessageProducer;
import fgm.queryfunction.QueryFunction;
import fgm.safefunction.SafeFunction;
import fgm.worker.windowoperator.Window;
import fgm.worker.statistics.LocalStatistic;
import org.apache.kafka.clients.producer.Producer;
import java.util.concurrent.atomic.AtomicLong;

import java.util.concurrent.atomic.AtomicBoolean;

public class FgmNodeStatistics<T extends Vector> {
    //the last time the leader executed the punctuation code
    private Long lastTimeStampPunctuation;
    //the max stream time timestamp after each state vector update
    private Long maxTimeStamp;
    private double theta;
    private double zi;
    private int ci;
    private final GlobalStatistic<T> E;
    private SafeFunction<T> safeFunction;
    private final QueryFunction<T> queryFunction;
    private final LocalStatistic<T> Si;
    private final LocalStatistic<T> Ei;
    private String nodeId;
    private final IMessageProducer<String, DriftVectorMessage> driftMessagesProducer;
    private final IMessageProducer<String, PhiMessage> phiMessagesProducer;
    private final IMessageProducer<String, StatisticsMessage> statisticsMessageProducer;
    private int currentRound;
    private int currentSubRound;
    private final Window<T> window;
    private final IFgmFactory<T> factory;
    private final AtomicBoolean firstDriftSent = new AtomicBoolean(false);
    //how further than a window slide the watermark has advanced
    private final AtomicLong lastAdvancedBoundary = new AtomicLong(Long.MIN_VALUE);
    private boolean thetaEqualsZero = true;

    public FgmNodeStatistics(IFgmFactory<T> factory) {
        this.maxTimeStamp = 0L;
        this.lastTimeStampPunctuation = 0L;
        this.factory = factory;
        this.Ei = factory.getLocalStatistic();
        this.window = factory.getWindowOperator();
        if (window != null) {
            this.Si = this.window.getLocalStatistic();
        } else {
            this.Si = factory.getLocalStatistic();
        }
        this.E = factory.getGlobalStatistic();
        this.queryFunction = factory.getQueryFunction();
        this.safeFunction = factory.getSafeFunction();
        this.ci = 0;
        this.theta = 0.0d;
        this.zi = 0;
        this.currentRound = 1;
        this.currentSubRound = 0;
        this.driftMessagesProducer = factory.getDriftMessageProducer();
        this.phiMessagesProducer = factory.getPhiMessageProducer();
        this.statisticsMessageProducer = factory.getStatisticsMessageProducer();

    }

    public Window<T> getWindow() {
        return this.window;
    }

    public GlobalStatistic<T> getE() {
        return this.E;
    }

    public <K extends Vector> void setE(GlobalStatistic<K> globalEstimate) {
        this.E.setVector(globalEstimate.getVector());
    }

    public LocalStatistic<T> getEi() {
        return this.Ei;
    }

    public synchronized LocalStatistic<T> getSi() {
        return this.Si;
    }

    public double getTheta() {
        return this.theta;
    }

    public double getZi() {
        return this.zi;
    }

    public int getCi() {
        return this.ci;
    }

    public void setTheta(double theta) {
        this.theta = theta;
    }

    public void setZi(double zi) {
        this.zi = zi;
    }

    public LocalStatistic<T> getDrift() {
        LocalStatistic<T> result = this.factory.getLocalStatistic();
        result.setVector(this.Si.getVector().subtract(Ei.getVector()));
        return result;
    }

    public SafeFunction<T> getSafeFunction() {
        return this.safeFunction;
    }

    public String getNodeId() {
        return this.nodeId;
    }

    public void setNodeId(String id) {
        this.nodeId = id;
    }

    public int getCurrentRound() {
        return this.currentRound;
    }

    public int getCurrentSubRound() {
        return this.currentSubRound;
    }

    public void setCi(int ci) {
        this.ci = ci;
    }

    public void setCurrentRound(int currentRound) {
        this.currentRound = currentRound;
    }

    public void setCurrentSubRound(int currentSubRound) {
        this.currentSubRound = currentSubRound;
    }

    public void setSafeFunction(SafeFunction<T> function) { // here we should create a class that contains Functions and constraints on E
        this.safeFunction = function;
    }

    public double getT() {
        return getQueryFunction().computeQuery(this.getE());
    }

    public QueryFunction<T> getQueryFunction() {
        return this.queryFunction;
    }

    public IMessageProducer<String, DriftVectorMessage> getDriftMessagesProducer() {
        return this.driftMessagesProducer;
    }

    public IMessageProducer<String, PhiMessage> getPhiMessagesProducer() {
        return this.phiMessagesProducer;
    }

    public IMessageProducer<String, StatisticsMessage> getStatisticsMessageProducer() {
        return this.statisticsMessageProducer;
    }

    public BaseConfiguration getBaseConfiguration() {
        return this.factory.getBaseCfg();
    }

    public Long getMaxTimeStamp() {
        return this.maxTimeStamp;
    }

    public void setMaxTimeStamp(Long timeStamp) {
        int compareResult = this.maxTimeStamp.compareTo(timeStamp);
        if (compareResult < 0) {
            this.maxTimeStamp = timeStamp;
        }
    }

    public Long getLastTimeStampPunctuation() {
        return this.lastTimeStampPunctuation;
    }

    public void setLastTimeStampPunctuation(Long lastTimeStampPunctuation) {
        this.lastTimeStampPunctuation = lastTimeStampPunctuation;
    }

    public boolean tryMarkFirstDriftSent() {
        return firstDriftSent.compareAndSet(false, true);
    }

    public AtomicLong lastAdvancedBoundary() { return lastAdvancedBoundary; }

    public boolean isThetaEqualsZero() { return thetaEqualsZero; }

    public void setThetaEqualsZero(boolean thetaEqualsZero) {this.thetaEqualsZero = thetaEqualsZero;}
}

