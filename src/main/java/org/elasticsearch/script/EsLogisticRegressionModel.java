package org.elasticsearch.script;

import org.dmg.pmml.RegressionModel;
import org.elasticsearch.common.collect.Tuple;

public class EsLogisticRegressionModel extends EsModelEvaluator {

    public EsLogisticRegressionModel(RegressionModel model) {
        super(model);
    }

    @Override
    public String evaluate(Tuple<int[], double[]> featureValues) {
        double val = linearFunction(featureValues, intercept, coefficients);
        double prob = 1 / (1 + Math.exp(-1.0 * val));
        return prob > 0.5 ? classes.v1() : classes.v2();
    }
}
