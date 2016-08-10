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

package org.elasticsearch.script.models;

import org.elasticsearch.script.modelinput.VectorModelInput;

import java.util.HashMap;
import java.util.Map;

public class EsLogisticRegressionModel extends EsModelEvaluator<VectorModelInput> {

    private final double[] coefficients;
    private final double intercept;
    private final String[] classes;

    public EsLogisticRegressionModel(double[] coefficients,
                                     double intercept, String[] classes) {
        this.coefficients = coefficients;
        this.intercept = intercept;
        this.classes = classes;
    }

    @Override
    public Map<String, Object> evaluateDebug(VectorModelInput modelInput) {
        double val = linearFunction(modelInput, intercept, coefficients);
        return prepareResult(val);
    }

    @Override
    public Object evaluate(VectorModelInput modelInput) {
        double val = linearFunction(modelInput, intercept, coefficients);
        double prob = 1 / (1 + Math.exp(-1.0 * val));
        return prob > 0.5 ? classes[0] : classes[1];
    }

    protected Map<String, Object> prepareResult(double val) {
        // TODO: this should be several classes really...
        double prob = 1 / (1 + Math.exp(-1.0 * val));
        String classValue = prob > 0.5 ? classes[0] : classes[1];
        Map<String, Object> result = new HashMap<>();
        result.put("class", classValue);
        Map<String, Object> probs = new HashMap<>();
        probs.put(classes[0], prob);
        probs.put(classes[1], 1.0 - prob);
        result.put("probs", probs);
        return result;
    }

    private double linearFunction(VectorModelInput modelInput, double intercept, double[] coefficients) {
        double val = 0.0;
        val += intercept;
        for (int i = 0; i < modelInput.getSize(); i++) {
            val += modelInput.getValue(i) * coefficients[modelInput.getIndex(i)];
        }
        return val;
    }

}
