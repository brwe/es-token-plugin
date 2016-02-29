/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script;

import org.dmg.pmml.NumericPredictor;
import org.dmg.pmml.RegressionModel;
import org.dmg.pmml.RegressionTable;
import org.elasticsearch.common.collect.Tuple;

import java.util.List;


public abstract class EsRegressionModelEvaluator implements EsModelEvaluator{
    double[] coefficients;
    double intercept;
    Tuple<String, String> classes;
    public EsRegressionModelEvaluator(RegressionModel regressionModel) {
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


