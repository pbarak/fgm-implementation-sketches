package fgm.queryfunction.implementation;

import fgm.common.statevector.FastAGMS;
import fgm.common.GlobalStatistic;
import fgm.queryfunction.QueryFunction;
import org.apache.commons.math3.stat.descriptive.rank.Median;

import java.util.Arrays;

public class FastAGMSQueryFunction<T extends FastAGMS> implements QueryFunction<T> {

    @Override
    public Double computeQuery(GlobalStatistic<T> globalStatistic) {  //this is the query function of the global estimate
        return medianOfInnerProducts(globalStatistic.getVector()); //according to the paper this cast is guaranteed  tp
    }

    @Override
    public Double computeQuery(T vector) {  //this is the query function of the global estimate
        return medianOfInnerProducts(vector); //according to the paper this cast is guaranteed  tp
    }


    private double medianOfInnerProducts(T vector) {
        //self-join size estimate
        int depth = vector.depth();
        double[] innerProductsArray = new double[depth];

        double update = 0.0;
        int j = 0;
        for (int i = 0; i < depth; i++) {

            double[] sketchColumn = vector.getSketchColumn(i);
            while (j < sketchColumn.length) {
                update = update + sketchColumn[j] * sketchColumn[j];
                j++;
            }
            innerProductsArray[i] = update;
            update = 0.0;
            j = 0;
        }
        Arrays.sort(innerProductsArray);
        Median median = new Median();
        return median.evaluate(innerProductsArray);
    }

}
