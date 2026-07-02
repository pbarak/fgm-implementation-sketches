package fgm.queryfunction.implementation;


import fgm.common.GlobalStatistic;
import fgm.common.statevector.HashMapState;
import fgm.queryfunction.QueryFunction;

public class NormHashMapStateQFunction<T extends HashMapState> implements QueryFunction<T> {

    public Double computeQuery(GlobalStatistic<T> globalStatistic) {  //this is the query function of the global estimate
        // The statistic’s concrete vector is a HashMapState
        HashMapState state = globalStatistic.getVector();

        double sumSq = 0.0;
        for (Double freq : state.getState().values()) {
            sumSq += freq * freq;
        }
        return Math.sqrt(sumSq);
    }

    public Double computeQuery(T vector) {  //this is the query function of the global estimate
        // The statistic’s concrete vector is a HashMapState

        double sumSq = 0.0;
        for (Double freq : vector.getState().values()) {
            sumSq += freq * freq;
        }
        return Math.sqrt(sumSq);
    }
}

