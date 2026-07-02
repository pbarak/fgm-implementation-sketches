package fgm.coordinator.collectorsinterfaces.implementation.processors.state;

import fgm.common.GlobalStatistic;
import fgm.common.Vector;
import fgm.factory.specs.IFgmFactory;
import fgm.queryfunction.QueryFunction;
import fgm.safefunction.SafeFunction;
import fgm.coordinator.collectorsinterfaces.implementation.processors.driftcollection.model.DriftVectorsCollection;
import fgm.coordinator.collectorsinterfaces.implementation.processors.driftcollection.model.EisCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class FgmCoordinatorState<T extends Vector> {
    int numberOfNodes; //this is the num of sites
    double theta;
    double ePsi;
    double psi;
    int globalCounter;
    int currentRound;
    int currentSubround;
    boolean started;
    GlobalStatistic<T> E; //global estimated statistic
    SafeFunction<T> safeFunction;
    QueryFunction<T> queryFunction;
    DriftVectorsCollection<T> drifts;
    EisCollection<T> Eis;
    PhiMessageCollection phis;
    ArrayList<String> siteIds;
    private static final Logger logger = LoggerFactory.getLogger(FgmCoordinatorState.class);

    public FgmCoordinatorState() {
    }

    public FgmCoordinatorState(IFgmFactory<T> factory) {
        logger.info("ENTERED in FgmCoordinator State initialization");
        numberOfNodes = factory.getBaseCfg().getNumberOfNodes();
        theta = 0.0d;
        ePsi = factory.getBaseCfg().ePsi();
        psi = 0.0f;
        globalCounter = 0;
        currentRound = 1;
        currentSubround = 0;
        started = false;
        E = factory.getGlobalStatistic();
        safeFunction = factory.getSafeFunction();
        queryFunction = factory.getQueryFunction();
        drifts = new DriftVectorsCollection<>();
        Eis = new EisCollection<>();
        phis = new PhiMessageCollection();
        siteIds = new ArrayList<>();
    }

    public int getNumberOfNodes() {
        return this.numberOfNodes;
    }

    public double getTheta() {
        return this.theta;
    }

    public double getEPsi() {
        return this.ePsi;
    }

    public double getPsi() {
        return this.psi;
    }

    public int getGlobalC() {
        return globalCounter;
    }

    public int getCurrentRound() {
        return this.currentRound;
    }

    public int getCurrentSubround() {
        return this.currentSubround;
    }

    public boolean getIfStarted() {
        return this.started;
    }

    public GlobalStatistic<T> getE() {
        return this.E;
    }

    public void setE(GlobalStatistic<T> E) {this.E = E;}

    public SafeFunction<T> getSafeFunction() {
        return this.safeFunction;
    }

    public QueryFunction<T> getQueryFunction() {
        return this.queryFunction;
    }

    public DriftVectorsCollection<T> getDriftCollection() {
        return this.drifts;
    }

    public EisCollection<T> getEisCollection() {
        return this.Eis;
    }

    public PhiMessageCollection getPhis() {
        return this.phis;
    }

    public void setGlobalC(int globalCounter) {
        this.globalCounter = globalCounter;
    }

    public void setPsi(double psi) {
        this.psi = psi;
    }

    public void setCurrentRound(int round) {
        this.currentRound = round;
    }

    public void setCurrentSubround(int currentSubround) {
        this.currentSubround = currentSubround;
    }

    public void setTheta(double theta) {
        this.theta = theta;
    }

    public void setStarted(boolean started) {
        this.started = started;
    }

    public double computePhiWithZeroDrift(double threshold) {
        return getSafeFunction().computePhiWithZeroDrift(getE().getVector(), threshold);
    }

    public double computeQuery() {
        return getQueryFunction().computeQuery(getE());
    }

}

