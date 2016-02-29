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

import org.dmg.pmml.NaiveBayesModel;
import org.elasticsearch.common.collect.Tuple;

public class EsNaiveBayesModel implements EsModelEvaluator {

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
    public String evaluate(Tuple<int[], double[]> featureValues) {

        double valClass0 = EsRegressionModelEvaluator.linearFunction(featureValues, pis[0], thetas[0]);
        double valClass1 = EsRegressionModelEvaluator.linearFunction(featureValues, pis[1], thetas[1]);
        return valClass0 > valClass1 ? labels[0] : labels[1];
    }

    public String evaluate(double[] featureValues) {
        double valClass0 = EsRegressionModelEvaluator.linearFunction(featureValues, pis[0], thetas[0]);
        double valClass1 = EsRegressionModelEvaluator.linearFunction(featureValues, pis[1], thetas[1]);
        return valClass0 > valClass1 ? labels[0] : labels[1];
    }
}
