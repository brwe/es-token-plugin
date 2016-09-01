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

import org.elasticsearch.ml.modelinput.VectorModelInput;

import java.util.HashMap;
import java.util.Map;

public class EsLinearSVMModel extends EsRegressionModelEvaluator {

    public EsLinearSVMModel(double[] coefficients,
                            double intercept, String[] classes) {
        super(coefficients, intercept, classes);
    }

    protected Map<String, Object> prepareResult(double val) {
        String classValue = val > 0 ? classes[0] : classes[1];
        Map<String, Object> result = new HashMap<>();
        result.put("class", classValue);
        return result;
    }

    @Override
    public Map<String, Object> evaluateDebug(VectorModelInput modelInput) {
        double val = linearFunction(modelInput);
        return prepareResult(val);
    }

    @Override
    public String evaluate(VectorModelInput modelInput) {
        double val = linearFunction(modelInput);
        return val > 0 ? classes[0] : classes[1];
    }
}
