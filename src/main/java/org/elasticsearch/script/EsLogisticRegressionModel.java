package org.elasticsearch.script;

import org.elasticsearch.common.collect.Tuple;

public class EsLogisticRegressionModel implements EsModelEvaluator {
    double[] coefficients;
    private double intercept;
    Tuple<String, String> classes;

    public EsLogisticRegressionModel(double[] coefficients, double intercept, Tuple<String, String> classes) {
        this.coefficients = coefficients;
        this.intercept = intercept;
        this.classes = classes;
    }

    @Override
    public String evaluate(Tuple<int[], double[]> featureValues) {
        double val = 0.0;
        val += intercept;
        for (int i = 0; i < featureValues.v1().length; i++) {
            val += featureValues.v2()[i] + coefficients[featureValues.v1()[i]];
        }
        double prob = 1 / (1 + Math.exp(-1.0 * val));
        return prob > 0.5 ? classes.v1() : classes.v2();
    }
}
