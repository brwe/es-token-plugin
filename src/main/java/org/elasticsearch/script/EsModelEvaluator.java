package org.elasticsearch.script;

import org.dmg.pmml.NumericPredictor;
import org.dmg.pmml.RegressionModel;
import org.dmg.pmml.RegressionTable;
import org.elasticsearch.common.collect.Tuple;

import java.util.List;


public abstract class EsModelEvaluator {
    double[] coefficients;
    double intercept;
    Tuple<String, String> classes;
    public EsModelEvaluator(RegressionModel regressionModel) {
        RegressionTable regressionTable = regressionModel.getRegressionTables().get(0);
        List<NumericPredictor> numericPredictors = regressionTable.getNumericPredictors();
        double[] coefficients = new double[numericPredictors.size()];
        int i = 0;
        for (NumericPredictor numericPredictor : numericPredictors) {
            coefficients[i] = numericPredictor.getCoefficient();
            i++;
        }
        this.coefficients = coefficients;
        this.intercept = regressionTable.getIntercept();
        this.classes = new Tuple<>(regressionModel.getRegressionTables().get(0).getTargetCategory(), regressionModel.getRegressionTables().get(1).getTargetCategory());
    }
    abstract  public String evaluate(Tuple<int[], double[]> featureValues);

    protected static double linearFunction(Tuple<int[], double[]> featureValues, double intercept, double[] coefficients) {
        double val = 0.0;
        val += intercept;
        for (int i = 0; i < featureValues.v1().length; i++) {
            val += featureValues.v2()[i] * coefficients[featureValues.v1()[i]];
        }
        return val;
    }
}


