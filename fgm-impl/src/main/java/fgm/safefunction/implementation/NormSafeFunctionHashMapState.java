package fgm.safefunction.implementation;

import fgm.common.statevector.HashMapState;
import fgm.safefunction.SafeFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class NormSafeFunctionHashMapState<T extends HashMapState> implements SafeFunction<T> {

    private static final Logger logger =
            LoggerFactory.getLogger(NormSafeFunctionHashMapState.class);
    private static final double EPS = 0.05;  //todo
    private static final double ZERO_DOUBLE_VALUE = 0.0d;

    public NormSafeFunctionHashMapState() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public double computePhiWithZeroDrift(
            T E,
            Double T_val
    ) {
        T xState = (T) createEmptyState();
        return computePhi(xState, E, T_val);
    }

    @Override
    public double computePhi(
            T X,
            T E,
            double T_val
    ) {
        double normE = norm(E.getState().entrySet());
        double normX = norm(X.getState().entrySet());

        HashMapState normalizedE = getNormalized(E, normE);
        double dot = dotProduct(X, normalizedE);

        double phi1 = -1*(1 + EPS)*normE + norm(vector_addition(X, E).entrySet());
        double phi2 = -1*1*EPS * normE - dot;


        return Math.max(phi1, phi2);
    }

    private HashMapState createEmptyState() {
        return HashMapState.empty();
    }

    private double dotProduct(HashMapState a, HashMapState b) {
        Map<Long, Double> ma = a.getState();
        Map<Long, Double> mb = b.getState();

        double product = ZERO_DOUBLE_VALUE;
        for(Long key: ma.keySet()){
            if(mb.containsKey(key)){
                product += ma.get(key) * mb.get(key);
            }
        }

        return product;
    }

//    private double dotProduct(HashMapState a, HashMapState b) {
//        Map<Long, Double> ma = a.getState();
//        Map<Long, Double> mb = b.getState();
//
//        // Iterate over the smaller map for efficiency
//        if (ma.size() > mb.size()) {
//            Map<Long, Double> tmp = ma;
//            ma = mb;
//            mb = tmp;
//        }
//
//        double sum = ZERO_DOUBLE_VALUE;
//        for (Map.Entry<Long, Double> e : ma.entrySet()) {
//            Double vb = mb.get(e.getKey());
//            if (vb != null) {
//                sum += e.getValue() * vb;
//            }
//        }
//        return sum;
//    }

    private double normSquared(HashMapState v) {
        double sum = ZERO_DOUBLE_VALUE;
        for (Double val : v.getState().values()) {
            sum += val * val;
        }
        return sum;
    }

    private Map<Long, Double> vector_addition(HashMapState x, HashMapState e) {
        Map<Long, Double> result  =  new HashMap<>(x.getState());
        for (Map.Entry<Long, Double> entry : e.getState().entrySet()) {
            result.put(entry.getKey(), result.getOrDefault(entry.getKey(), ZERO_DOUBLE_VALUE) + entry.getValue());
        }
        return result;
    }

    public static Double norm(Iterable<Map.Entry<Long, Double>> iterable) {
        double result = 0d;
        for (Map.Entry<Long, Double> entry : iterable)
            result += entry.getValue() * entry.getValue();
        return Math.sqrt(result);
    }

//    private double sumNorm(HashMapState x, HashMapState e) {
//        Map<Long, Double> mx = x.getState();
//        Map<Long, Double> me = e.getState();
//
//        double sum = ZERO_DOUBLE_VALUE;
//
//        // (x_k + e_k)^2 for keys in x
//        for (Map.Entry<Long, Double> ex : mx.entrySet()) {
//            double vx = ex.getValue();
//            double ve = me.getOrDefault(ex.getKey(), ZERO_DOUBLE_VALUE);
//            double s = vx + ve;
//            sum += s * s;
//        }
//
//        // add e-only keys
//        for (Map.Entry<Long, Double> ee : me.entrySet()) {
//            Long k = ee.getKey();
//            if (!mx.containsKey(k)) {
//                double ve = ee.getValue();
//                sum += ve * ve;
//            }
//        }
//
//        return Math.sqrt(sum);
//    }

    private HashMapState getNormalized(HashMapState E, double norm) {
        if(norm == 0)
            throw new ArithmeticException("norm value cannot be 0 when normalizing");
        HashMapState normalizedE = new HashMapState();
        for (Map.Entry<Long, Double> e : E.getState().entrySet()) {
            double ve = e.getValue();
            double normalizedV = ve / norm;
            normalizedE.getState().put(e.getKey(), normalizedV);
        }
        return normalizedE;
    }

}
