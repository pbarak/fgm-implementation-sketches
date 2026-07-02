package safefunction.hashmapstate;

import fgm.common.statevector.HashMapState;
import fgm.queryfunction.implementation.NormHashMapStateQFunction;
import fgm.safefunction.implementation.NormSafeFunctionHashMapState;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class HashMapStateNormTest {
private static final Double UNIT_FREQUENCY = 1.0d;
    @Test
    void test_norm_upperBound() {

            HashMapState E = new HashMapState();

            // fill the sketch with random values
            for (int w = 0; w < 200; w++) {
                E.update(randomLong1to(1000000L), UNIT_FREQUENCY);
            }

            NormHashMapStateQFunction<HashMapState> hashMapStateQFunction = new NormHashMapStateQFunction<>();
            NormSafeFunctionHashMapState<HashMapState> normSafeFunction = new NormSafeFunctionHashMapState<>();

            double Emed = hashMapStateQFunction.computeQuery(E);

            System.out.println("the safe function of E is:" + normSafeFunction.computePhi(HashMapState.empty(), E, Emed));
            System.out.println("the norm function of E is:" + Emed);
            assertTrue(0.0 >= normSafeFunction.computePhi(HashMapState.empty(), E, Emed));


            int count_inA = 0, count_notInA = 0, count_inZ = 0, count_notInZ = 0;

            //for (int j = 0; j < 10; j++) {
                HashMapState X = new HashMapState();

                // X = E + rand
                for (int w = 0; w < 20; w++)
                    X.update(randomLong1to(1000000L), UNIT_FREQUENCY);

                int inA = (hashMapStateQFunction.computeQuery(vector_addition(X,E)) <= 1.05 * Emed)
                        && (0.95 * Emed <= hashMapStateQFunction.computeQuery(vector_addition(X,E))) ? 1 : 0;
                if (inA == 1) count_inA++;
                else count_notInA++;

                System.out.println("Phi is:" + normSafeFunction.computePhi(X, E, Emed));
                System.out.println("the norm function of X is:" + hashMapStateQFunction.computeQuery(vector_addition(X,E)));
                int inZ = (normSafeFunction.computePhi(X, E, Emed) <= 0) ? 1 : 0;
                if (inZ == 1) count_inZ++;
                else count_notInZ++;
                //assert inZ >= inA;
            //}
            System.out.println("Upper bound test:");
            System.out.println("in A: " + count_inA + ", not in A: " + count_notInA);
            System.out.println("in Z: " + count_inZ + ", not in Z: " + count_notInZ);
    }

    public static Long randomLong1to(Long max) {
        // inclusive 1..max
        return ThreadLocalRandom.current().nextLong(1, max + 1);
    }

    private HashMapState vector_addition(HashMapState x, HashMapState e) {
        Map<Long, Double> result  =  new HashMap<>(x.getState());
        for (Map.Entry<Long, Double> entry : e.getState().entrySet()) {
            result.put(entry.getKey(), result.getOrDefault(entry.getKey(), 0.0d) + entry.getValue());
        }
        return new HashMapState(result);
    }
}
