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
import org.dmg.pmml.DataField;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.GaussianDistribution;
import org.dmg.pmml.MiningField;
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
import org.elasticsearch.script.models.EsNaiveBayesModelWithMixedInput.Function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public class NaiveBayesModelFactory extends ModelFactory<VectorModelInput, NaiveBayesModel> {

    public NaiveBayesModelFactory() {
        super(NaiveBayesModel.class);
    }


    @Override
    public ModelAndModelInputEvaluator<VectorModelInput> buildFromPMML(NaiveBayesModel naiveBayesModel, DataDictionary dataDictionary,
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
                String finalFieldName = bayesInput.getFieldName().getValue();
                PMMLVectorRange vectorRange = getFeatureEntryFromNaiveBayesMModel(naiveBayesModel, dataDictionary,
                        transformationDictionary,
                        finalFieldName, indexCounter, bayesInput, types);
                vectorRanges.add(vectorRange);
                indexCounter += vectorRange.size();
                targetValueStats.add(bayesInput.getTargetValueStats());
            }
            VectorModelInputEvaluator vectorPMML = new VectorModelInputEvaluator(vectorRanges);

            EsModelEvaluator<VectorModelInput> model = buildEsNaiveBayesModel(naiveBayesModel, types);
            return new ModelAndModelInputEvaluator<>(vectorPMML, model);
        } else {
            throw new UnsupportedOperationException("Naive does not support the following parameters yet: "
                    + " functionName:" + naiveBayesModel.getFunctionName().value());
        }
    }

    private EsModelEvaluator<VectorModelInput> buildEsNaiveBayesModel(NaiveBayesModel naiveBayesModel, Map<String, OpType> types) {
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
        List<List<Function>> functionLists = initFunctions(naiveBayesModel, types, classCounts, classIndexMap, classLabels);
        Function[][] functions = new Function[functionLists.size()][functionLists.get(0).size()];
        classCounter = 0;
        for (List<Function> classFunctions : functionLists) {
            int functionCounter = 0;
            for (Function classFunction : classFunctions) {
                functions[classCounter][functionCounter] = classFunction;
                functionCounter++;
            }
            classCounter++;
        }
        return new EsNaiveBayesModelWithMixedInput(classLabels, functions, classPriors);
    }

    private List<List<Function>> initFunctions(NaiveBayesModel naiveBayesModel, Map<String, OpType> types, double[] classCounts,
                               Map<String, Integer> classIndexMap, String[] classLabels) {
        List<List<Function>> functionLists = new ArrayList<>();
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
                    functionLists.get(classIndexMap.get(classAssignment)).add(new Function.GaussFunction(gaussianDistribution.getVariance(),
                            gaussianDistribution.getMean()));
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
                        functionLists.get(classIndex).add(new Function.ProbFunction(prob, threshold));
                    }
                }
            } else {
                throw new UnsupportedOperationException("cannot deal with bayes input that is not categorical and also not continuous");
            }
        }
        return functionLists;
    }

    private PMMLVectorRange getFeatureEntryFromNaiveBayesMModel(NaiveBayesModel model,
                                                               DataDictionary dataDictionary,
                                                               TransformationDictionary transformationDictionary,
                                                               String fieldName,
                                                               int indexCounter, BayesInput bayesInput, Map<String, OpType> types) {

        List<DerivedField> allDerivedFields = ProcessPMMLHelper.getAllDerivedFields(model, transformationDictionary);
        List<DerivedField> derivedFields = new ArrayList<>();
        String rawFieldName = ProcessPMMLHelper.getDerivedFields(fieldName, allDerivedFields, derivedFields);
        DataField rawField = ProcessPMMLHelper.getRawDataField(dataDictionary, rawFieldName);
        MiningField miningField = ProcessPMMLHelper.getMiningField(model, rawFieldName);
        PMMLVectorRange featureEntries = getFieldVector(indexCounter, derivedFields, rawField, miningField, bayesInput, types);
        return featureEntries;
    }

    private PMMLVectorRange getFieldVector(int indexCounter, List<DerivedField> derivedFields, DataField rawField,
                                          MiningField miningField, BayesInput bayesInput, Map<String, OpType> types) {
        PMMLVectorRange featureEntries;
        OpType opType;
        if (derivedFields.size() == 0) {
            opType = rawField.getOpType();
        } else {
            opType = derivedFields.get(0).getOpType();
        }

        if (opType.value().equals("continuous")) {
            featureEntries = new PMMLVectorRange.ContinousSingleEntryVectorRange(rawField, miningField, derivedFields.toArray(new
                    DerivedField[derivedFields
                    .size()]));
        } else if (opType.value().equals("categorical")) {
            featureEntries = new PMMLVectorRange.SparseCategoricalVectorRange(rawField, miningField, derivedFields.toArray(new
                    DerivedField[derivedFields
                    .size()]));
        } else {
            throw new UnsupportedOperationException("Only implemented continuous and categorical variables so far.");
        }

        if (opType.equals(OpType.CATEGORICAL)) {
            // sort values first
            TreeSet<String> sortedValues = new TreeSet<>();
            for (PairCounts pairCount : bayesInput.getPairCounts()) {
                sortedValues.add(pairCount.getValue());
            }
            for (String value : sortedValues) {
                featureEntries.addVectorEntry(indexCounter, value);
                indexCounter++;
            }
        } else if (opType.equals(OpType.CONTINUOUS)) {
            featureEntries.addVectorEntry(indexCounter, "dummyValue");
        } else {
            throw new UnsupportedOperationException("only supporting categorical and continuous input for naive bayes so far");
        }
        types.put(featureEntries.getLastDerivedFieldName(), opType);

        return featureEntries;
    }

}
