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

import org.dmg.pmml.BayesInput;
import org.dmg.pmml.ContinuousDistribution;
import org.dmg.pmml.GaussianDistribution;
import org.dmg.pmml.NaiveBayesModel;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PairCounts;
import org.dmg.pmml.TargetValueCount;
import org.dmg.pmml.TargetValueCounts;
import org.dmg.pmml.TargetValueStat;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public class EsNaiveBayesModelWithMixedInput extends EsNumericInputModelEvaluator {

    private Function[][] functions;
    private double[] classPriors;
    private String[] classLabels;
    Map<String, Integer> classIndexMap;

    public EsNaiveBayesModelWithMixedInput(NaiveBayesModel naiveBayesModel, Map<String, OpType> types) {
        double[] classCounts = initClassPriorsAndLabels(naiveBayesModel);
        initFunctions(naiveBayesModel, types, classCounts);
    }

    private void initFunctions(NaiveBayesModel naiveBayesModel, Map<String, OpType> types, double[] classCounts) {
        List<List<Function>> functionLists = new ArrayList<>();
        for (int i = 0; i < classLabels.length; i++) {
            functionLists.add(new ArrayList<Function>());
        }
        double threshold = naiveBayesModel.getThreshold();
        for (BayesInput bayesInput : naiveBayesModel.getBayesInputs()) {
            String fieldName = bayesInput.getFieldName().getValue();
            if (types.containsKey(fieldName) == false) {
                throw new UnsupportedOperationException("Cannot determine type of field " + bayesInput.getFieldName().getValue() +
                        "probably messed up parsing");
            }
            if (types.get(fieldName).equals(OpType.CONTINUOUS)) {

                for (TargetValueStat targetValueStat : bayesInput.getTargetValueStats()) {
                    ContinuousDistribution continuousDistribution = targetValueStat.getContinuousDistribution();
                    if (continuousDistribution instanceof GaussianDistribution == false) {
                        throw new UnsupportedOperationException("Only Gaussian distribution implemented so fay for naive bayes model");
                    }
                    GaussianDistribution gaussianDistribution = (GaussianDistribution) continuousDistribution;
                    String classAssignment = targetValueStat.getValue();
                    functionLists.get(classIndexMap.get(classAssignment)).add(new GaussFunction(gaussianDistribution.getVariance(), gaussianDistribution
                            .getMean()));
                }
            } else if (types.get(fieldName).equals(OpType.CATEGORICAL)) {
                TreeMap<String, TargetValueCounts> sortedValues = new TreeMap<>();
                for (PairCounts pairCount : bayesInput.getPairCounts()) {
                    sortedValues.put(pairCount.getValue(), pairCount.getTargetValueCounts());
                }
                for (Map.Entry<String, TargetValueCounts> counts: sortedValues.entrySet()) {
                    for (TargetValueCount targetValueCount : counts.getValue()) {
                        Integer classIndex = classIndexMap.get(targetValueCount.getValue());
                        double prob = targetValueCount.getCount() / classCounts[classIndex];
                        functionLists.get(classIndex).add(new ProbFunction(prob, threshold));
                    }
                }
            } else {
                throw new UnsupportedOperationException("cannot deal with bayes input that is not categorical and also not continuous");
            }
        }
        functions = new Function[functionLists.size()][functionLists.get(0).size()];
        int classCounter = 0;
        for (List<Function> classFunctions : functionLists) {
            int functionCounter = 0;
            for (Function classFunction : classFunctions) {
                functions[classCounter][functionCounter] = classFunction;
                functionCounter++;
            }
            classCounter++;
        }
    }

    private double[] initClassPriorsAndLabels(NaiveBayesModel naiveBayesModel) {
        classIndexMap = new HashMap<>();
        // get class priors
        int numClasses = naiveBayesModel.getBayesOutput().getTargetValueCounts().getTargetValueCounts().size();
        // sort first
        TreeMap<String, Double> sortedClassLabelsAndCounts = new TreeMap<>();
        classPriors = new double[numClasses];
        double[] classCounts = new double[numClasses];
        classLabels = new String[numClasses];
        double sumCounts = 0;
        for (TargetValueCount targetValueCount : naiveBayesModel.getBayesOutput().getTargetValueCounts().getTargetValueCounts()) {
            sortedClassLabelsAndCounts.put(targetValueCount.getValue(), targetValueCount.getCount());
            sumCounts += targetValueCount.getCount();
        }
        int classCounter = 0;
        for (Map.Entry<String, Double> classCount : sortedClassLabelsAndCounts.entrySet()) {
            classPriors[classCounter] = Math.log(classCount.getValue() / sumCounts);
            classLabels[classCounter] = classCount.getKey();
            classCounts[classCounter] = classCount.getValue();
            classIndexMap.put(classCount.getKey(), classCounter);
            classCounter++;
        }
        return classCounts;
    }

    @Override
    public Map<String, Object> evaluate(Tuple<int[], double[]> featureValues) {
        double[] classProbs = new double[classLabels.length];
        System.arraycopy(classPriors, 0, classProbs, 0, classProbs.length);
        for (int i = 0; i < featureValues.v1().length; i++) {
            for (int j = 0; j < classProbs.length; j++) {
                classProbs[j] += functions[j][featureValues.v1()[i]].eval(featureValues.v2()[i]);
            }
        }
        return prepareResult(classProbs);
    }

    protected Map<String, Object> prepareResult(double... val) {
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

    public Map<String, Object> evaluate(double[] featureValues) {
        throw new UnsupportedOperationException("Naive Bayes with miced inputs not implemented for dense vectors");
    }

    public abstract static class Function {
        abstract double eval(double value);
    }

    public static class GaussFunction extends Function {
        double variance;
        double mean;
        double varianceFactor;

        public GaussFunction(double variance, double mean) {

            this.variance = variance;
            this.mean = mean;
            varianceFactor = Math.log(Math.sqrt(2 * Math.PI * variance));
        }

        double eval(double value) {
            return -Math.pow((value - mean), 2) / (2 * variance) - varianceFactor;
        }
    }

    public static class ProbFunction extends Function {
        double prob;

        public ProbFunction(double prob, double threshold) {

            if (prob == 0.0) {
                this.prob = Math.log(threshold);
            } else {
                this.prob = Math.log(prob);
            }
        }

        double eval(double value) {
            return prob;
        }
    }

}
