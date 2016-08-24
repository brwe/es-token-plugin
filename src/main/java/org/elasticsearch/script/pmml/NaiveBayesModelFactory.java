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

package org.elasticsearch.script.pmml;


import org.dmg.pmml.BayesInput;
import org.dmg.pmml.ContinuousDistribution;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.GaussianDistribution;
import org.dmg.pmml.NaiveBayesModel;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PairCounts;
import org.dmg.pmml.TargetValueCount;
import org.dmg.pmml.TargetValueCounts;
import org.dmg.pmml.TargetValueStat;
import org.dmg.pmml.TargetValueStats;
import org.dmg.pmml.TransformationDictionary;
import org.elasticsearch.script.modelinput.ModelAndModelInputEvaluator;
import org.elasticsearch.script.modelinput.PMMLVectorRange;
import org.elasticsearch.script.modelinput.VectorModelInput;
import org.elasticsearch.script.modelinput.VectorModelInputEvaluator;
import org.elasticsearch.script.modelinput.VectorRange;
import org.elasticsearch.script.models.EsModelEvaluator;
import org.elasticsearch.script.models.EsNaiveBayesModelWithMixedInput;
import org.elasticsearch.script.models.EsNaiveBayesModelWithMixedInput.GaussFunction;
import org.elasticsearch.script.models.EsNaiveBayesModelWithMixedInput.ProbFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.DoubleUnaryOperator;

public class NaiveBayesModelFactory extends ModelFactory<VectorModelInput, String, NaiveBayesModel> {

    public NaiveBayesModelFactory() {
        super(NaiveBayesModel.class);
    }


    @Override
    public ModelAndModelInputEvaluator<VectorModelInput, String> buildFromPMML(NaiveBayesModel naiveBayesModel,
                                                                               DataDictionary dataDictionary,
                                                                               TransformationDictionary transformationDictionary) {
        if (naiveBayesModel.getFunctionName().value().equals("classification")) {
            // for each Bayes input
            // find the whole tranform pipeline (cp glm)
            // create vector range
            // append Ijk/Tk
            List<VectorRange> vectorRanges = new ArrayList<>();
            List<TargetValueStats> targetValueStats = new ArrayList<>();
            int indexCounter = 0;
            Map<String, OpType> types = new HashMap<>();
            for (BayesInput bayesInput : naiveBayesModel.getBayesInputs()) {
                PMMLVectorRange vectorRange = ProcessPMMLHelper.extractVectorRange(naiveBayesModel, dataDictionary,
                        transformationDictionary, bayesInput.getFieldName().getValue(), () -> {
                            // sort values first
                            TreeSet<String> sortedValues = new TreeSet<>();
                            for (PairCounts pairCount : bayesInput.getPairCounts()) {
                                sortedValues.add(pairCount.getValue());
                            }
                            return sortedValues;
                        }, indexCounter, types);
                vectorRanges.add(vectorRange);
                indexCounter += vectorRange.size();
                targetValueStats.add(bayesInput.getTargetValueStats());
            }
            VectorModelInputEvaluator vectorPMML = new VectorModelInputEvaluator(vectorRanges);

            EsModelEvaluator<VectorModelInput, String> model = buildEsNaiveBayesModel(naiveBayesModel, types);
            return new ModelAndModelInputEvaluator<>(vectorPMML, model);
        } else {
            throw new UnsupportedOperationException("Naive does not support the following parameters yet: "
                    + " functionName:" + naiveBayesModel.getFunctionName().value());
        }
    }

    private EsModelEvaluator<VectorModelInput, String> buildEsNaiveBayesModel(NaiveBayesModel naiveBayesModel, Map<String, OpType> types) {
        Map<String, Integer> classIndexMap = new HashMap<>();
        // get class priors
        int numClasses = naiveBayesModel.getBayesOutput().getTargetValueCounts().getTargetValueCounts().size();
        // sort first
        TreeMap<String, Double> sortedClassLabelsAndCounts = new TreeMap<>();
        double[] classPriors = new double[numClasses];
        double[] classCounts = new double[numClasses];
        String[] classLabels = new String[numClasses];
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
        List<List<DoubleUnaryOperator>> functionLists = initFunctions(naiveBayesModel, types, classCounts, classIndexMap, classLabels);
        DoubleUnaryOperator[][] functions = new DoubleUnaryOperator[functionLists.size()][functionLists.get(0).size()];
        classCounter = 0;
        for (List<DoubleUnaryOperator> classFunctions : functionLists) {
            int functionCounter = 0;
            for (DoubleUnaryOperator classFunction : classFunctions) {
                functions[classCounter][functionCounter] = classFunction;
                functionCounter++;
            }
            classCounter++;
        }
        return new EsNaiveBayesModelWithMixedInput(classLabels, functions, classPriors);
    }

    private List<List<DoubleUnaryOperator>> initFunctions(NaiveBayesModel naiveBayesModel, Map<String, OpType> types, double[] classCounts,
                                                     Map<String, Integer> classIndexMap, String[] classLabels) {
        List<List<DoubleUnaryOperator>> functionLists = new ArrayList<>();
        for (int i = 0; i < classLabels.length; i++) {
            functionLists.add(new ArrayList<>());
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
                    functionLists.get(classIndexMap.get(classAssignment)).add(
                            new GaussFunction(gaussianDistribution.getVariance(), gaussianDistribution.getMean()));
                }
            } else if (types.get(fieldName).equals(OpType.CATEGORICAL)) {
                TreeMap<String, TargetValueCounts> sortedValues = new TreeMap<>();
                for (PairCounts pairCount : bayesInput.getPairCounts()) {
                    sortedValues.put(pairCount.getValue(), pairCount.getTargetValueCounts());
                }
                for (Map.Entry<String, TargetValueCounts> counts : sortedValues.entrySet()) {
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
        return functionLists;
    }

}
