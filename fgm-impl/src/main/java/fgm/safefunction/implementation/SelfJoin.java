package fgm.safefunction.implementation;

import fgm.common.cfg.BaseConfiguration;
import fgm.common.statevector.FastAGMS;
import fgm.factory.FactoryInjection;
import fgm.factory.specs.IFgmFactory;
import fgm.safefunction.SafeFunction;

import java.util.*;

import org.apache.commons.math3.util.CombinatoricsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelfJoin<T extends FastAGMS> implements SafeFunction<T> {
//we create for local drift vector a boolean array with the same dimensionality as that of the depth of the sketch.
// if each row's inner product violates the higher (and lower) bound outlier(s) it is set to false.
//we count each number of true elements, (n - p1) and (n - p2) as per paper, and we calculate all the possible combinations.

    private static final long serialVersionUID = 1L; //assign a long value
    private static final Logger LOGGER = LoggerFactory.getLogger(SelfJoin.class);
    IFgmFactory<?> factory;
    BaseConfiguration baseConfiguration;

    public SelfJoin() {
        factory = FactoryInjection.createFactory();
        baseConfiguration = factory.getBaseCfg();
    }

    @Override
    public double computePhi(T X, T E, double T) {

        //the computation depends on the specialization of Local and Global Statistics classes that is FastAGMS

        FastAGMS driftVector = X; //according to paper it is guaranteed for the cast to be successful
        FastAGMS estimation = E;
        int depth = estimation.depth();
        boolean[] upperBoundBooleanArray = new boolean[depth];
        boolean[] lowerBoundBooleanArray = new boolean[depth];

        for (int i = 0; i < depth; i++) {
            upperBoundBooleanArray[i] = true;
            lowerBoundBooleanArray[i] = true;
        }

        int p1 = 0; // p1
        int p2 = 0; // p2


        for (int i = 0; i < estimation.depth(); i++) {
            if (innerProduct(estimation.getSketchColumn(i)) >= baseConfiguration.getUpperThreshold() * T) {
                upperBoundBooleanArray[i] = false;
                p1++;
            }
        }

        int[] tempUpperBoundBooleanArray = new int[estimation.depth() - p1];
        int index1 = 0;
        for (int i = 0; i < estimation.depth(); i++) {
            if (upperBoundBooleanArray[i]) {
                tempUpperBoundBooleanArray[index1++] = i;
            }
        }

        for (int i = 0; i < estimation.depth(); i++) {
            if (innerProduct(estimation.getSketchColumn(i)) <= baseConfiguration.getLowerThreshold() * T) {
                lowerBoundBooleanArray[i] = false;
                p2++;
            }
        }

        int[] tempLowerBoundBooleanArray = new int[estimation.depth() - p2];
        int index2 = 0;
        for (int i = 0; i < estimation.depth(); i++) {
            if (lowerBoundBooleanArray[i]) {
                tempLowerBoundBooleanArray[index2++] = i;
            }
        }

        int majority = majorityNumber(depth); // n depth of sketch

        // find all the combinations of n - p + 1 - majority of those indexes of booleanArray that are true. (m)
        boolean firstFound = false;
        double maxPhiPlusValue = 0.0;
        int kPlus = estimation.depth() - p1 + 1 - majority;
        if (kPlus > 0) {
            Iterator<int[]> iterator1 = CombinatoricsUtils.combinationsIterator(estimation.depth() - p1, kPlus);
            while (iterator1.hasNext()) {
                int[] combination1 = iterator1.next();
                double temp = phiPlus(combination1, driftVector, estimation, T, tempUpperBoundBooleanArray);
                if (!firstFound) {
                    maxPhiPlusValue = temp;
                    firstFound = true;
                } else {
                    if (maxPhiPlusValue < temp) {
                        maxPhiPlusValue = temp;
                    }
                }
            }
        }
        int m = 0;

        boolean firstFound1 = false;
        double maxPhiMinusValue = 0.0;

        int kMinus = estimation.depth() - p2 + 1 - majority;
        if (kMinus > 0) {
            Iterator<int[]> iterator2 = CombinatoricsUtils.combinationsIterator(estimation.depth() - p2, kMinus);
            while (iterator2.hasNext()) {
                m = m + 1;
                int[] combination2 = iterator2.next();
                double temp = phiMinus(combination2, driftVector, estimation, T, tempLowerBoundBooleanArray);
                if (!firstFound1) {
                    maxPhiMinusValue = temp;
                    firstFound1 = true;
                } else {
                    if (maxPhiMinusValue < temp) {
                        maxPhiMinusValue = temp;
                    }
                }
            }
        }

        if (maxPhiMinusValue <= maxPhiPlusValue) {
            return maxPhiPlusValue;
        } else {
            return maxPhiMinusValue;
        }

    }

    public double innerProduct(double[] column) {
        double update = 0.0;
        int i = 0;
        while (i < column.length) {
            update = update + column[i] * column[i];
            i++;
        }
        return update;
    }

    public int majorityNumber(int depth) {
        if ((depth % 2) == 0) {
            return (depth / 2) + 1;
        } else {
            return (int) Math.ceil(depth / 2.0);
        }
    }

    public double phiPlus(int[] columns, FastAGMS X, FastAGMS E, double T, int[] tempUpperBoundBooleanArray) {
        double update1 = 0.0;
        double update2 = 0.0;
        for (int i = 0; i < columns.length; i++) {
            update1 =
                    update1 +
                            Math.abs(phiPlusZeroArgumentColumnFunction(E, tempUpperBoundBooleanArray[columns[i]], T))
                                    * phiPlusColumnFunction(X.getSketchColumn(tempUpperBoundBooleanArray[columns[i]]), E, tempUpperBoundBooleanArray[columns[i]], T);

            double tmp = phiPlusZeroArgumentColumnFunction(E, tempUpperBoundBooleanArray[columns[i]], T);
            update2 = update2 + Math.abs(tmp) * Math.abs(tmp);
        }
        return update1 / Math.sqrt(update2);
    }


    public double phiZeroDriftPlus(int[] columns, FastAGMS E, double T, int[] tempUpperBoundBooleanArray) {
        double update1 = 0.0;
        double update2 = 0.0;
        for (int i = 0; i < columns.length; i++) {
            update1 =
                    update1 +
                            Math.abs(phiPlusZeroArgumentColumnFunction(E, tempUpperBoundBooleanArray[columns[i]], T))
                                    * phiPlusZeroArgumentColumnFunction(E, tempUpperBoundBooleanArray[columns[i]], T);

            double tmp = phiPlusZeroArgumentColumnFunction(E, tempUpperBoundBooleanArray[columns[i]], T);
            update2 = update2 + Math.abs(tmp) * Math.abs(tmp);
        }
        return update1 / Math.sqrt(update2);
    }

    public double phiMinus(int[] columns, FastAGMS X, FastAGMS E, double T, int[] tempLowerBoundBooleanArray) {
        double update1 = 0.0;
        double update2 = 0.0;
        for (int i = 0; i < columns.length; i++) {
            update1 =
                    update1 +
                            Math.abs(phiMinusZeroArgumentColumnFunction(E, tempLowerBoundBooleanArray[columns[i]], T))
                                    * phiMinusColumnFunction(X.getSketchColumn(tempLowerBoundBooleanArray[columns[i]]), E, tempLowerBoundBooleanArray[columns[i]], T);
            double tmp = phiMinusZeroArgumentColumnFunction(E, tempLowerBoundBooleanArray[columns[i]], T);
            update2 = update2 + Math.abs(tmp) * Math.abs(tmp);
        }
        return update1 / Math.sqrt(update2);
    }

    public double phiZeroDriftMinus(int[] columns, FastAGMS E, double T, int[] tempLowerBoundBooleanArray) {
        double update1 = 0.0;
        double update2 = 0.0;
        for (int i = 0; i < columns.length; i++) {
            update1 =
                    update1 +
                            Math.abs(phiMinusZeroArgumentColumnFunction(E, tempLowerBoundBooleanArray[columns[i]], T))
                                    * phiMinusZeroArgumentColumnFunction(E, tempLowerBoundBooleanArray[columns[i]], T);
            double tmp = phiMinusZeroArgumentColumnFunction(E, tempLowerBoundBooleanArray[columns[i]], T);
            update2 = update2 + Math.abs(tmp) * Math.abs(tmp);
        }
        return update1 / Math.sqrt(update2);
    }

    public double phiPlusColumnFunction(double[] x, FastAGMS E, int column, double T) {
        double update1 = 0.0;

        int j = 0;
        double[] Ei = E.getSketchColumn(column);
        while (j < x.length && j < Ei.length) {
            update1 = update1 + (x[j] + Ei[j]) * (x[j] + Ei[j]);
            j++;
        }

        return (Math.sqrt(update1) - Math.sqrt(baseConfiguration.getUpperThreshold() * T));
    }

    public double phiPlusZeroArgumentColumnFunction(FastAGMS E, int column, double T) {
        double update1 = 0.0;

        int j = 0;
        double[] Ei = E.getSketchColumn(column);
        while (j < Ei.length) {
            update1 = update1 + (Ei[j]) * (Ei[j]);
            j++;
        }
        return (Math.sqrt(update1) - Math.sqrt(baseConfiguration.getUpperThreshold() * T));
    }

    public double phiMinusColumnFunction(double[] x, FastAGMS E, int column, double T) {
        double update1 = 0.0;

        double[] Ei = E.getSketchColumn(column);
        assert (x.length == Ei.length);
        double update2 = 0.0;
        int i = 0;
        while (i < Ei.length) {
            update2 = update2 + Ei[i] * Ei[i];
            i++;
        }

        int j = 0;
        while (j < x.length && j < Ei.length) {
            update1 = update1 + (x[j] + Ei[j]) * (Ei[j] / Math.sqrt(update2));
            j++;
        }
        return (Math.sqrt(baseConfiguration.getLowerThreshold() * T) - (update1));
    }


    public double phiMinusZeroArgumentColumnFunction(FastAGMS E, int column, double T) {
        double update1 = 0.0;

        int j = 0;
        double[] Ei = E.getSketchColumn(column);
        while (j < Ei.length) {
            update1 = update1 + (Ei[j]) * (Ei[j]);
            j++;
        }

        int i = 0;
        double update2 = 0.0d;
        while (i < Ei.length) {
            update2 = update2 + (Ei[i] / Math.sqrt(update1)) * (Ei[i]);
            i++;
        }


        return Math.sqrt(baseConfiguration.getLowerThreshold() * T) - (update2);
    }

    public double computePhiWithZeroDrift(T E, Double T_val) {
        T estimation = E;
        int depth = estimation.depth();
        boolean[] upperBoundBooleanArray = new boolean[depth];
        boolean[] lowerBoundBooleanArray = new boolean[depth];

        for (int i = 0; i < depth; i++) {
            upperBoundBooleanArray[i] = true;
            lowerBoundBooleanArray[i] = true;
        }

        int p1 = 0; // p1
        int p2 = 0; // p2


        for (int i = 0; i < estimation.depth(); i++) {
            if (innerProduct(estimation.getSketchColumn(i)) >= baseConfiguration.getUpperThreshold() * T_val) {
                upperBoundBooleanArray[i] = false;
                p1++;
            }
        }
        int[] tempUpperBoundBooleanArray = new int[estimation.depth() - p1];
        int index1 = 0;
        for (int i = 0; i < estimation.depth(); i++) {
            if (upperBoundBooleanArray[i]) {
                tempUpperBoundBooleanArray[index1] = i;
                index1++;
            }
        }

        for (int i = 0; i < estimation.depth(); i++) {
            if (innerProduct(estimation.getSketchColumn(i)) <= baseConfiguration.getLowerThreshold() * T_val) {
                lowerBoundBooleanArray[i] = false;
                p2++;
            }
        }

        int[] tempLowerBoundBooleanArray = new int[estimation.depth() - p2];
        int index2 = 0;
        for (int i = 0; i < estimation.depth(); i++) {
            if (lowerBoundBooleanArray[i]) {
                tempLowerBoundBooleanArray[index2] = i;
                index2 = index2 + 1;
            }
        }

        int majority = majorityNumber(depth); // n depth of sketch

        // find all of the combinations of n - p + 1 - majority of those indexes of booleanArray that

        // are true. (m)

        double maxPhiPlusValue = 0.0;
        boolean started = false;

        int kPlus = estimation.depth() - p1 + 1 - majority;
        if (kPlus > 0) {
            Iterator<int[]> iterator1 = CombinatoricsUtils.combinationsIterator(estimation.depth() - p1, kPlus);
            while (iterator1.hasNext()) {
                final int[] combination1 = iterator1.next();
                double temp = phiZeroDriftPlus(combination1, estimation, T_val, tempUpperBoundBooleanArray);

                if (!started) {
                    maxPhiPlusValue = temp;
                    started = true;
                } else {
                    if (maxPhiPlusValue < temp) {
                        maxPhiPlusValue = temp;
                    }
                }
            }
        }

        double maxPhiMinusValue = 0.0;
        boolean started1 = false;

        int m = 0;

        int kMinus = estimation.depth() - p2 + 1 - majority;
        if (kMinus > 0) {
            Iterator<int[]> iterator2 = CombinatoricsUtils.combinationsIterator(estimation.depth() - p2, kMinus);
            while (iterator2.hasNext()) {
                m = m + 1;
                final int[] combination2 = iterator2.next();
                double temp = phiZeroDriftMinus(combination2, estimation, T_val, tempLowerBoundBooleanArray);
                if (!started1) {
                    maxPhiMinusValue = temp;
                    started1 = true;
                } else {
                    if (maxPhiMinusValue < temp) {
                        maxPhiMinusValue = temp;
                    }
                }
            }
        }

        if (maxPhiMinusValue <= maxPhiPlusValue) {
            return maxPhiPlusValue;
        } else {
            return maxPhiMinusValue;
        }
    }
}