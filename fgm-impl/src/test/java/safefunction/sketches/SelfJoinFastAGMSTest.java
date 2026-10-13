package safefunction.sketches;

import fgm.common.cfg.BaseConfiguration;
import fgm.common.statevector.FastAGMS;
import fgm.factory.FactoryInjection;
import fgm.queryfunction.implementation.FastAGMSQueryFunction;
import fgm.safefunction.implementation.SelfJoin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Locale;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/*
 * Self-join safe function (paper, Section 5.1.1) on a Fast-AGMS sketch.
 *
 * Reference point: E is a 5 x 10 sketch with every cell = 1.0, so every row has ||E[i]||^2 = 10 and
 * Q(E) = median = 10. With thresholds 1.1 / 0.9 the admissible region is A = { S : 9 <= Q(S) <= 11 }.
 *
 * The bound tests use a drift X whose cells all hold the same value c. Then E + X = (1 + c) * E,
 * Q(E + X) = 10 * (1 + c)^2, and the safe function reduces to
 *
 *     phi(X) = sqrt(3) * max( sqrt(10) * (1 + c) - sqrt(11),     upper bound (phi+)
 *                             sqrt(9)  - sqrt(10) * (1 + c) )    lower bound (phi-)
 *
 * where 3 is the size of the row subsets, d - majority + 1. It changes sign exactly where Q(E + X)
 * crosses a threshold:
 *
 *     upper:  c* = sqrt(1.1) - 1 = +0.0488088...   ->   0.048 inside,   0.049 outside
 *     lower:  c* = sqrt(0.9) - 1 = -0.0513167...   ->  -0.051 inside,  -0.052 outside
 *
 * Sign convention: phi takes the DRIFT X (not the state), and phi(X) <= 0 means "X is in the safe zone Z".
 * Safety means inZ => inA: the zone may be smaller than A but never larger.
 */
class SelfJoinFastAGMSTest {

    private static final int DEPTH = 5;
    private static final int WIDTH = 10;

    /** Size of the row subsets in the median composition: d - majority + 1 = 5 - 3 + 1 (E has no outlier rows). */
    private static final int SUBSET_SIZE = 3;

    // Drift cell values on either side of each boundary (for thresholds 1.1 / 0.9).
    private static final double UPPER_INSIDE = 0.048;    // Q(E + X) = 10.98304 <= 11
    private static final double UPPER_OUTSIDE = 0.049;   // Q(E + X) = 11.00401 >  11
    private static final double LOWER_INSIDE = -0.051;   // Q(E + X) =  9.00601 >=  9
    private static final double LOWER_OUTSIDE = -0.052;  // Q(E + X) =  8.98704 <   9

    private static final double TOL = 1e-12;

    private final FastAGMSQueryFunction<FastAGMS> queryFunction = new FastAGMSQueryFunction<>();

    private FastAGMS E;
    private double Emed;
    private SelfJoin<FastAGMS> sz;
    private double upper;   // 1 + epsilon
    private double lower;   // 1 - epsilon

    @BeforeEach
    void setUp() {
        E = constantSketch(1.0);
        Emed = queryFunction.computeQuery(E);
        sz = new SelfJoin<>();

        // the same configuration SelfJoin takes its thresholds from
        BaseConfiguration cfg = FactoryInjection.createFactory().getBaseCfg();
        upper = cfg.getUpperThreshold();
        lower = cfg.getLowerThreshold();
    }

    /**
     * With no drift the state is E itself, strictly inside A, so phi(0) must be strictly negative
     * (Definition 2.1). Its size is the slack of the round: -sqrt(3) * (sqrt(11) - sqrt(10)) = -0.26733707...
     */
    @Test
    void test_zeroDrift_isStrictlyInsideTheSafeZone() {
        double phi0 = sz.computePhiWithZeroDrift(E, Emed);
        double phi0ViaComputePhi = sz.computePhi(constantSketch(0.0), E, Emed);

        System.out.printf(Locale.ROOT, "Q(E) = %.4f, phi(0) = %.16f%n", Emed, phi0);

        assertTrue(phi0 < 0.0, "phi(0) must be < 0, was " + phi0);
        assertEquals(phi0, phi0ViaComputePhi, TOL, "computePhi(0, E) and computePhiWithZeroDrift(E) must agree");
        assertEquals(expectedPhi(0.0), phi0, TOL, "phi(0) = -sqrt(3) * (sqrt(upper * Q(E)) - sqrt(Q(E)))");
    }

    /**
     * Upper bound: Arrays.fill(row, 0.048) keeps Q(E + X) = 10.98304 under 11 and phi(X) negative,
     * Arrays.fill(row, 0.049) gives Q(E + X) = 11.00401 over 11 and phi(X) positive.
     */
    @Test
    void test_sj_upperBound() {
        assumeThresholdsAreTenPercent();

        Probe inside = probe(constantSketch(UPPER_INSIDE));
        Probe outside = probe(constantSketch(UPPER_OUTSIDE));

        System.out.printf(Locale.ROOT, "Upper bound test: A ends at Q = %.4f%n", upper * Emed);

        // 0.048: still admissible, still in the safe zone
        assertTrue(inside.inA, "c = 0.048 must be in A, Q(E + X) = " + inside.query);
        assertTrue(inside.phi < 0.0, "c = 0.048 must give phi < 0, was " + inside.phi);
        assertEquals(expectedPhi(UPPER_INSIDE), inside.phi, TOL, "phi at c = 0.048");      // -0.00443024...

        // 0.049: the median is over the upper threshold and phi has become positive
        assertFalse(outside.inA, "c = 0.049 must be outside A, Q(E + X) = " + outside.query);
        assertTrue(outside.phi > 0.0, "c = 0.049 must give phi > 0, was " + outside.phi);
        assertEquals(expectedPhi(UPPER_OUTSIDE), outside.phi, TOL, "phi at c = 0.049");    // +0.00104698...

        // walking away from E, 0.049 is the first step on which phi is positive
        assertEquals(49, sweep(40, 56), "first c (in 1/1000) with phi > 0");

        // and the exact point of the sign change is where Q(E + X) = upper * Q(E)
        double boundary = Math.sqrt(upper) - 1.0;
        double signChange = findSignChange(UPPER_INSIDE, UPPER_OUTSIDE);
        printSignChange(signChange, boundary);

        assertEquals(boundary, signChange, TOL, "phi must change sign at c = sqrt(upper) - 1");
        assertEquals(0.0, phiOfConstantDrift(boundary), 1e-9, "phi on the boundary");
    }

    /**
     * Lower bound, same demonstration with negative drift: Arrays.fill(row, -0.051) keeps Q(E + X) = 9.00601
     * over 9 and phi(X) negative, Arrays.fill(row, -0.052) gives Q(E + X) = 8.98704 under 9 and phi(X) positive.
     */
    @Test
    void test_sj_lowerBound() {
        assumeThresholdsAreTenPercent();

        Probe inside = probe(constantSketch(LOWER_INSIDE));
        Probe outside = probe(constantSketch(LOWER_OUTSIDE));

        System.out.printf(Locale.ROOT, "Lower bound test: A ends at Q = %.4f%n", lower * Emed);

        // -0.051: still admissible, still in the safe zone
        assertTrue(inside.inA, "c = -0.051 must be in A, Q(E + X) = " + inside.query);
        assertTrue(inside.phi < 0.0, "c = -0.051 must give phi < 0, was " + inside.phi);
        assertEquals(expectedPhi(LOWER_INSIDE), inside.phi, TOL, "phi at c = -0.051");     // -0.00173464...

        // -0.052: the median is under the lower threshold and phi has become positive
        assertFalse(outside.inA, "c = -0.052 must be outside A, Q(E + X) = " + outside.query);
        assertTrue(outside.phi > 0.0, "c = -0.052 must give phi > 0, was " + outside.phi);
        assertEquals(expectedPhi(LOWER_OUTSIDE), outside.phi, TOL, "phi at c = -0.052");   // +0.00374257...

        // walking away from E, -0.052 is the first step on which phi is positive
        assertEquals(-52, sweep(-44, -60), "first c (in 1/1000) with phi > 0");

        // and the exact point of the sign change is where Q(E + X) = lower * Q(E)
        double boundary = Math.sqrt(lower) - 1.0;
        double signChange = findSignChange(LOWER_INSIDE, LOWER_OUTSIDE);
        printSignChange(signChange, boundary);

        assertEquals(boundary, signChange, TOL, "phi must change sign at c = sqrt(lower) - 1");
        assertEquals(0.0, phiOfConstantDrift(boundary), 1e-9, "phi on the boundary");
    }

    /**
     * Both bounds, random drifts in every direction. The property that must never break is safety:
     * {@code phi(X) <= 0} implies E + X in A. The opposite does not hold, the zone is a strict subset of A.
     */
    @Test
    void test_selfjoin() {
        Random rand = new Random(42);   // fixed seed, reproducible run
        int count_inA = 0, count_notInA = 0, count_inZ = 0, count_notInZ = 0, count_inAButNotInZ = 0;

        // cells uniform in [-radius, radius]; a radius just above the slack |phi(0)| lands drifts in all three regions
        double radius = 1.1 * Math.abs(sz.computePhiWithZeroDrift(E, Emed));

        for (int j = 0; j < 1000; j++) {
            Probe p = probe(randomSketch(rand, radius));

            if (p.inA) count_inA++; else count_notInA++;
            if (p.inZ) count_inZ++; else count_notInZ++;
            if (p.inA && !p.inZ) count_inAButNotInZ++;

            assertTrue(!p.inZ || p.inA,
                    "safety violated: phi(X) = " + p.phi + " <= 0 but Q(E + X) = " + p.query + " is outside A");
        }

        System.out.println("Both bounds test:");
        System.out.println("in A: " + count_inA + ", not in A: " + count_notInA);
        System.out.println("in Z: " + count_inZ + ", not in Z: " + count_notInZ);
        System.out.println("in A but not in Z: " + count_inAButNotInZ);

        assertTrue(count_inZ > 0 && count_notInA > 0 && count_inAButNotInZ > 0,
                "the sample should contain drifts inside Z, outside A, and in between");
    }

    // ------------------------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------------------------

    /** Q(E + X) and phi(X) for one drift, plus the two membership flags. */
    private static final class Probe {
        final double query;
        final double phi;
        final boolean inA;   // lower * Q(E) <= Q(E + X) <= upper * Q(E)
        final boolean inZ;   // phi(X) <= 0

        Probe(double query, double phi, boolean inA, boolean inZ) {
            this.query = query;
            this.phi = phi;
            this.inA = inA;
            this.inZ = inZ;
        }
    }

    private Probe probe(FastAGMS X) {
        // S = E + X
        FastAGMS S = new FastAGMS(DEPTH, WIDTH);
        S.setCounts(X.getCounts());
        S.add(E);

        double query = queryFunction.computeQuery(S);
        double phi = sz.computePhi(X, E, Emed);

        return new Probe(query, phi, lower * Emed <= query && query <= upper * Emed, phi <= 0.0);
    }

    private double phiOfConstantDrift(double c) {
        return sz.computePhi(constantSketch(c), E, Emed);
    }

    /** Closed form of phi for a drift whose cells all equal c, given that every cell of E is 1.0 (needs c > -1). */
    private double expectedPhi(double c) {
        double rowNorm = (1.0 + c) * Math.sqrt(WIDTH);              // ||E[i] + X[i]||, also its projection on E[i]
        double phiPlusRow = rowNorm - Math.sqrt(upper * Emed);      // phi+_i(X[i])
        double phiMinusRow = Math.sqrt(lower * Emed) - rowNorm;     // phi-_i(X[i])
        return Math.sqrt(SUBSET_SIZE) * Math.max(phiPlusRow, phiMinusRow);
    }

    /**
     * Walks c = milli / 1000 from fromMilli to toMilli (inclusive, either direction), prints one row per step
     * and checks on every step that phi matches the closed form, that phi grows as the drift moves away from E,
     * and that {@code phi(X) <= 0} exactly when Q(E + X) is admissible: along this line Z and A end at the
     * same point.
     *
     * @return the first milli value at which phi is positive, or 0 if it never became positive
     */
    private int sweep(int fromMilli, int toMilli) {
        int step = fromMilli <= toMilli ? 1 : -1;
        int firstOutside = 0;
        double previousPhi = Double.NEGATIVE_INFINITY;

        System.out.printf(Locale.ROOT, "%8s  %14s  %17s  %-5s  %s%n", "c", "Q(E+X)", "phi(X)", "inA", "inZ");
        for (int milli = fromMilli; milli != toMilli + step; milli += step) {
            double c = milli / 1000.0;
            Probe p = probe(constantSketch(c));
            System.out.printf(Locale.ROOT, "%8.3f  %14.8f  %+17.12f  %-5b  %b%s%n",
                    c, p.query, p.phi, p.inA, p.inZ, (firstOutside == 0 && p.phi > 0.0) ? "   <- sign change" : "");

            assertEquals(expectedPhi(c), p.phi, TOL, "closed form of phi at c = " + c);
            assertTrue(p.phi > previousPhi, "phi must grow as the drift moves away from E, c = " + c);
            assertTrue(p.inA == p.inZ, "along this line phi <= 0 exactly when Q(E + X) is admissible, c = " + c);

            if (firstOutside == 0 && p.phi > 0.0) firstOutside = milli;
            previousPhi = p.phi;
        }
        return firstOutside;
    }

    /** Bisection on c between a drift inside the zone and one outside it. */
    private double findSignChange(double inside, double outside) {
        assertTrue(phiOfConstantDrift(inside) <= 0.0 && phiOfConstantDrift(outside) > 0.0,
                "phi must be <= 0 at " + inside + " and > 0 at " + outside);

        for (int i = 0; i < 60; i++) {
            double mid = 0.5 * (inside + outside);
            if (phiOfConstantDrift(mid) <= 0.0) inside = mid;
            else outside = mid;
        }
        return 0.5 * (inside + outside);
    }

    private void printSignChange(double signChange, double boundary) {
        System.out.printf(Locale.ROOT, "phi changes sign at c = %.12f, threshold crossing is at c = %.12f%n%n",
                signChange, boundary);
    }

    private void assumeThresholdsAreTenPercent() {
        assumeTrue(Math.abs(upper - 1.1) < 1e-9 && Math.abs(lower - 0.9) < 1e-9,
                "0.048/0.049 and -0.051/-0.052 lie on either side of the boundaries only for thresholds 1.1 / 0.9,"
                        + " configured: " + upper + " / " + lower);
    }

    /** depth x width sketch with every cell = value. */
    private static FastAGMS constantSketch(double value) {
        FastAGMS sketch = new FastAGMS(DEPTH, WIDTH);
        for (int w = 0; w < sketch.depth(); w++) {
            Double[] row = new Double[sketch.buckets()];
            Arrays.fill(row, value);
            sketch.setSketchColumn(w, row);
        }
        return sketch;
    }

    /** depth x width sketch with cells uniform in [-radius, radius]. */
    private static FastAGMS randomSketch(Random rand, double radius) {
        FastAGMS sketch = new FastAGMS(DEPTH, WIDTH);
        for (int w = 0; w < sketch.depth(); w++) {
            Double[] row = new Double[sketch.buckets()];
            for (int j = 0; j < row.length; j++)
                row[j] = radius * (2.0 * rand.nextDouble() - 1.0);
            sketch.setSketchColumn(w, row);
        }
        return sketch;
    }
}