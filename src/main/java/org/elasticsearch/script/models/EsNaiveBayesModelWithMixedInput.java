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

public class EsNaiveBayesModelWithMixedInput extends EsModelEvaluator<VectorModelInput, String> {

    private final Function[][] functions;
    private final double[] classPriors;
    private final String[] classLabels;

    public EsNaiveBayesModelWithMixedInput(String[] classLabels, Function[][] functions, double[] classPriors) {
        this.functions = functions;
        this.classPriors = classPriors;
        this.classLabels = classLabels;
    }

    @Override
    public Map<String, Object> evaluateDebug(VectorModelInput modelInput) {
        double[] classProbs = getClassProbs(modelInput);
        return prepareResult(classProbs);
    }

    private double[] getClassProbs(VectorModelInput modelInput) {
        double[] classProbs = new double[classLabels.length];
        System.arraycopy(classPriors, 0, classProbs, 0, classProbs.length);
        for (int i = 0; i < modelInput.getSize(); i++) {
            for (int j = 0; j < classProbs.length; j++) {
                classProbs[j] += functions[j][modelInput.getIndex(i)].eval(modelInput.getValue(i));
            }
        }
        return classProbs;
    }

    @Override
    public String evaluate(VectorModelInput modelInput) {
        double[] classProbs = getClassProbs(modelInput);
        int bestClass = 0;
        // sum the values to get the actual probs
        double bestProb = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < classProbs.length; i++) {
            if (bestProb < classProbs[i]) {
                bestClass = i;
                bestProb = classProbs[i];
            }
        }
        return classLabels[bestClass];

    }

    private Map<String, Object> prepareResult(double... val) {
        int bestClass = 0;
        // sum the values to get the actual probs
        double sumProb = 0;
        double bestProb = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < val.length; i++) {
            if (bestProb < val[i]) {
                bestClass = i;
                bestProb = val[i];
            }
            sumProb += Math.exp(val[i]);
        }
        Map<String, Object> results = new HashMap<>();
        String classValue = classLabels[bestClass];
        results.put("class", classValue);
        Map<String, Double> probMap = new HashMap<>();
        for (int i = 0; i < val.length; i++) {
            probMap.put(classLabels[i], Math.exp(val[i]) / sumProb);
        }
        results.put("probs", probMap);
        return results;
    }

    public interface Function {
        double eval(double value);


        class GaussFunction implements Function {
            double variance;
            double mean;
            double varianceFactor;

            public GaussFunction(double variance, double mean) {

                this.variance = variance;
                this.mean = mean;
                varianceFactor = Math.log(Math.sqrt(2 * Math.PI * variance));
            }

            public double eval(double value) {
                return -Math.pow((value - mean), 2) / (2 * variance) - varianceFactor;
            }
        }

        class ProbFunction implements Function {
            double prob;

            public ProbFunction(double prob, double threshold) {

                if (prob == 0.0) {
                    this.prob = Math.log(threshold);
                } else {
                    this.prob = Math.log(prob);
                }
            }

            public double eval(double value) {
                return prob;
            }
        }

    }
}
