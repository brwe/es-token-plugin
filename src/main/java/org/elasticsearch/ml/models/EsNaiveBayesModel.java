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

package org.elasticsearch.ml.models;

import org.dmg.pmml.NaiveBayesModel;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.ml.modelinput.VectorModelInput;

import java.util.HashMap;
import java.util.Map;

public class EsNaiveBayesModel extends EsModelEvaluator<VectorModelInput, String> {

    private double[][] thetas;
    private double[] pis;
    private String[] labels;

    public EsNaiveBayesModel(NaiveBayesModel regressionModel) {
        throw new UnsupportedOperationException("not imeplemented yet");
    }

    public EsNaiveBayesModel(double thetas[][], double[] pis, String[] labels) {
        this.thetas = thetas;
        this.pis = pis;
        this.labels = labels;
    }

    @Override
    public String evaluate(VectorModelInput modelInput) {
        double valClass0 = linearFunction(modelInput, pis[0], thetas[0]);
        double valClass1 = linearFunction(modelInput, pis[1], thetas[1]);
        return valClass0 > valClass1 ? labels[0] : labels[1];
    }

    private Map<String, Object> prepareResult(double valClass0, double valClass1) {
        Map<String, Object> results = new HashMap<>();
        String classValue = valClass0 > valClass1 ? labels[0] : labels[1];
        results.put("class", classValue);
        return results;
    }

    @Override
    public Map<String, Object> evaluateDebug(VectorModelInput modelInput) {
        double valClass0 = linearFunction(modelInput, pis[0], thetas[0]);
        double valClass1 = linearFunction(modelInput, pis[1], thetas[1]);
        return prepareResult(valClass0, valClass1);
    }

    private static double linearFunction(VectorModelInput modelInput, double intercept, double[] coefficients) {
        double val = 0.0;
        val += intercept;
        for (int i = 0; i < modelInput.getSize(); i++) {
            val += modelInput.getValue(i) * coefficients[modelInput.getIndex(i)];
        }
        return val;
    }
}
