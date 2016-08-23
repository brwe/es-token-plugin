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


public abstract class EsRegressionModelEvaluator extends EsModelEvaluator<VectorModelInput, String> {

    protected final double[] coefficients;
    protected final double intercept;
    protected final String[] classes;

    public EsRegressionModelEvaluator(double[] coefficients, double intercept, String[] classes) {
        this.coefficients = coefficients;
        this.intercept = intercept;
        this.classes = classes;
    }

    protected double linearFunction(VectorModelInput modelInput) {
        double val = 0.0;
        val += intercept;
        for (int i = 0; i < modelInput.getSize(); i++) {
            val += modelInput.getValue(i) * coefficients[modelInput.getIndex(i)];
        }
        return val;
    }
}


