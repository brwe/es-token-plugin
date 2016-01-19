package org.elasticsearch.script;

import org.dmg.pmml.RegressionModel;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLoggerFactory;

public class EsLinearSVMModel extends EsModelEvaluator {

    public EsLinearSVMModel(RegressionModel regressionModel) {
        super(regressionModel);
    }

    @Override
    public String evaluate(Tuple<int[], double[]> featureValues) {
        double val = linearFunction(featureValues, intercept, coefficients);
        ESLoggerFactory.getRootLogger().info("value is for svm: {}", val);
        return val > 0.0 ? classes.v1() : classes.v2();
    }
}
