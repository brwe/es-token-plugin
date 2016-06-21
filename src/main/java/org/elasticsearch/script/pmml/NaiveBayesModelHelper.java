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
import org.dmg.pmml.DataField;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.NaiveBayesModel;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.PairCounts;
import org.dmg.pmml.TargetValueStats;
import org.elasticsearch.script.modelinput.PMMLVectorRange;
import org.elasticsearch.script.modelinput.VectorRange;
import org.elasticsearch.script.modelinput.VectorRangesToVectorPMML;
import org.elasticsearch.script.models.EsModelEvaluator;
import org.elasticsearch.script.models.EsNaiveBayesModelWithMixedInput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NaiveBayesModelHelper {


    public static PMMLModelScriptEngineService.FieldsToVectorAndModel getNaiveBayesFeaturesAndModel(PMML pmml, int modelNum) {
        NaiveBayesModel naiveBayesModel = (NaiveBayesModel) pmml.getModels().get(modelNum);
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
                PMMLVectorRange vectorRange = getFeatureEntryFromNaiveBayesMModel(pmml, 0, finalFieldName, indexCounter, bayesInput, types);
                vectorRanges.add(vectorRange);
                indexCounter += vectorRange.size();
                targetValueStats.add(bayesInput.getTargetValueStats());
            }
            VectorRangesToVectorPMML vectorPMML = new VectorRangesToVectorPMML(vectorRanges, indexCounter);

            EsModelEvaluator model = new EsNaiveBayesModelWithMixedInput(naiveBayesModel, types);
            return new PMMLModelScriptEngineService.FieldsToVectorAndModel(vectorPMML, model);
        } else {
            throw new UnsupportedOperationException("Naive does not support the following parameters yet: "
                    + " functionName:" + naiveBayesModel.getFunctionName().value());
        }
    }

    static PMMLVectorRange getFeatureEntryFromNaiveBayesMModel(PMML model, int modelIndex, String fieldName, int
            indexCounter, BayesInput bayesInput, Map<String, OpType> types) {
        if (model.getModels().get(modelIndex) instanceof NaiveBayesModel == false) {
            throw new UnsupportedOperationException("Can only do NaiveBayes so far");
        }

        List<DerivedField> allDerivedFields = ProcessPMMLHelper.getAllDerivedFields(model, modelIndex);
        List<DerivedField> derivedFields = new ArrayList<>();
        String rawFieldName = ProcessPMMLHelper.getDerivedFields(fieldName, allDerivedFields, derivedFields);
        DataField rawField = ProcessPMMLHelper.getRawDataField(model, rawFieldName);
        MiningField miningField = ProcessPMMLHelper.getMiningField(model, modelIndex, rawFieldName);


        PMMLVectorRange featureEntries = getFieldVector(indexCounter, derivedFields, rawField, miningField, bayesInput, types);
        return featureEntries;
    }

    static PMMLVectorRange getFieldVector(int indexCounter, List<DerivedField> derivedFields, DataField rawField,
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
            featureEntries = new PMMLVectorRange.SparseCategorical1OfKVectorRange(rawField, miningField, derivedFields.toArray(new
                    DerivedField[derivedFields
                    .size()]));
        } else {
            throw new UnsupportedOperationException("Only implemented continuous and categorical variables so far.");
        }

        if (opType.equals(OpType.CATEGORICAL)) {
            for (PairCounts pairCount : bayesInput.getPairCounts()) {
                featureEntries.addVectorEntry(indexCounter, pairCount.getValue());
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
