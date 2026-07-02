package safefunction.sketches;

import fgm.common.statevector.FastAGMS;
import fgm.queryfunction.implementation.FastAGMSQueryFunction;
import fgm.safefunction.implementation.SelfJoin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Random;

import static java.lang.Math.abs;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SelfJoinFastAGMSTest {

    /**
     * Test the self-join upper bound for accuracy.
     * This is done by taking random sketches and comparing
     * to the distance of the admissible region.
     */
    @Test
    void test_sj_upperBound() {

        for (int i = 0; i < 10; i++) {
            FastAGMS E = new FastAGMS(5, 10);

            // fill the sketch with random values
            for (int w = 0; w < E.depth(); w++)
                E.setSketchColumn(w, create_E_vector(E.buckets(), 1, 0));

            FastAGMSQueryFunction<FastAGMS> fastAGMSFastAGMSQueryFunction = new FastAGMSQueryFunction<>();

            double Emed = fastAGMSFastAGMSQueryFunction.computeQuery(E);
            System.out.println("Query function result is:" + Emed);


            SelfJoin<FastAGMS> sz = new SelfJoin<>();

            System.out.println("the safe function of E is:" + sz.computePhiWithZeroDrift(E, Emed));
            assertTrue(0.0 <= sz.computePhi(E, E, Emed));


            int count_inA = 0, count_notInA = 0, count_inZ = 0, count_notInZ = 0;

            for (int j = 0; j < 10; j++) {
                FastAGMS X = new FastAGMS(5, 10);

                // X = E + rand
                for (int w = 0; w < X.depth(); w++)
                    X.setSketchColumn(w, create_X_vector(X.buckets(), 1, 0));
                    //X.setSketchColumn(w, add(E.getSketchColumn(w), uniform_random_vector(X.buckets(), 1, 1000)));
                FastAGMS addition = new FastAGMS(5,10);
                addition.setCounts(X.getCounts());
                addition.add(E);


                System.out.println("Query function result is:" + fastAGMSFastAGMSQueryFunction.computeQuery(addition));
                int inA = (fastAGMSFastAGMSQueryFunction.computeQuery(addition) <= 1.1 * Emed)
                && (0.9 * Emed <= fastAGMSFastAGMSQueryFunction.computeQuery(addition)) ? 1 : 0;
                if (inA == 1) count_inA++;
                else count_notInA++;

                System.out.println("Phi is:" + sz.computePhi(X, E, Emed));
                int inZ = (sz.computePhi(X, E, Emed) <= 0) ? 1 : 0;
                if (inZ == 1) count_inZ++;
                else count_notInZ++;
                //assert inZ >= inA;
            }
            System.out.println("Upper bound test:");
            System.out.println("in A: " + count_inA + ", not in A: " + count_notInA);
            System.out.println("in Z: " + count_inZ + ", not in Z: " + count_notInZ);
        }
    }

    /**
     * Test the self-join lower bound for accuracy.
     * This is done by taking random sketches and comparing
     * to the distance of the admissible region.
     */
//    @Test
//    public void test_sj_lowerBound() {
//        FastAGMS E = new FastAGMS(5, 10);
//
//        for (int i = 0; i < 10; i++) {
//
//            // fill the sketch with random values
//            for (int w = 0; w < E.depth(); w++)
//                E.setSketchColumn(w, uniform_random_vector(E.depth(), 20, -10));
//
//            FastAGMSQueryFunction<FastAGMS> fastAGMSFastAGMSQueryFunction = new FastAGMSQueryFunction<>();
//
//            double Emed = fastAGMSFastAGMSQueryFunction.computeQuery(E);
//
//            SelfJoinAGMS.SelfJoinLowerBound sz;
//            sz = new SelfJoinLowerBound(E.getCounts(), 0.9 * Emed, true);
//
//
//            assertTrue(0.0 < sz.median(E.getCounts()));
//
//            // test 100 sketches from each type
//            int count_inA = 0, count_notInA = 0, count_inZ = 0, count_notInZ = 0;
//
//            while (count_notInA < 100 || count_inA < 100) {
//                FastAGMS X = new FastAGMS(5, 10);
//
//                // X = E + rand
//                for (int w = 0; w < X.depth(); w++)
//                    X.setSketchColumn(w, add(E.getSketchColumn(w), uniform_random_vector(X.depth(), 10, -5)));
//
//                int inA = (median(X.getCounts()) >= 0.9 * Emed) ? 1 : 0;
//                if (inA == 1) count_inA++;
//                else count_notInA++;
//
//                int inZ = (sz.median(X.getCounts()) > 0) ? 1 : 0;
//                if (inZ == 1) count_inZ++;
//                else count_notInZ++;
//
//                assert inZ <= inA;
//            }
//            System.out.println("Lower bound test:");
//            System.out.println("in A: " + count_inA + ", not in A: " + count_notInA);
//            System.out.println("in Z: " + count_inZ + ", not in Z: " + count_notInZ);
//        }
//    }
//
//    /**
//     * Testing the self-join complete safezone.
//     * Not implemented COMPARE_WITH_BOUNDING_BALLS segment
//     */
//    @Test
//    public void test_selfjoin() {
//        FastAGMS E = new FastAGMS(5, 10);
//
//        for (int i = 0; i < 10; i++) {
//
//            // fill the sketch with random values
//            for (int w = 0; w < E.depth(); w++)
//                E.setSketchColumn(w, uniform_random_vector(E.depth(), 20, -10));
//
//            double Emed = median(E.getCounts());
//
//            SelfJoinAGMS sz = new SelfJoinAGMS(E.getCounts(), 0.9 * Emed, 1.1 * Emed, true);
//
//
//            assertTrue(0.0 < sz.zeta(E.getCounts()));
//
//            // test 100 sketches from each type
//            int count_inA = 0, count_notInA = 0, count_inZ = 0, count_notInZ = 0;
//
//            for (int j = 0; j < 1000; j++) {
//                FastAGMS X = new FastAGMS(5, 10);
//
//                // X = E + rand
//                for (int w = 0; w < X.depth(); w++)
//                    X.setSketchColumn(w, add(E.getSketchColumn(w), uniform_random_vector(X.depth(), 4, -2)));//2*1.65, -1.65)));
//
//                int inA = (abs(median(X.getCounts()) - Emed) <= 0.1 * Emed) ? 1 : 0;
//                if (inA == 1) count_inA++;
//                else count_notInA++;
//
//                int inZ = (sz.zeta(X.getCounts()) > 0) ? 1 : 0;
//                if (inZ == 1) count_inZ++;
//                else count_notInZ++;
//
//                assert inZ <= inA;
//            }
//            System.out.println("Both bounds test:");
//            System.out.println("in A: " + count_inA + ", not in A: " + count_notInA);
//            System.out.println("in Z: " + count_inZ + ", not in Z: " + count_notInZ);
//        }



    private static Double[] uniform_random_vector(int n, double scale, double shift) {
        Random rand = new Random();
        Double[] res = new Double[n];
        for (int j = 0; j < n; j++)
            res[j] = (scale * rand.nextDouble()) + shift;
        return res;
    }

    private static Double[] create_E_vector(int n, double scale, double shift) {
        //Random rand = new Random();
        Double[] res = new Double[n];
        Arrays.fill(res, 1.0d);
        return res;
    }


    private static Double[] create_X_vector(int n, double scale, double shift) {
        //Random rand = new Random();
        Double[] res = new Double[n];
        Arrays.fill(res, 0.047d);
        return res;
    }


    private static Double[] add(Double[] col1, Double[] col2) {
        assert col1.length == col2.length;
        Double[] ret = new Double[col1.length];
        Arrays.fill(ret,0d);
        for(int d = 0; d < col1.length; d++)
            ret[d] = col1[d] + col2[d];
        return ret;
    }
}
