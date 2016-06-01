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

import org.dmg.pmml.DataField;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.GeneralRegressionModel;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.PPCell;
import org.dmg.pmml.Parameter;
import org.dmg.pmml.Predictor;
import org.elasticsearch.script.FieldToVector;
import org.elasticsearch.script.FieldsToVectorPMML;
import org.elasticsearch.script.PMMLFieldToVector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


public class GeneralizedLinearRegressionHelper {

    static PMMLFieldToVector getFieldVector(List<PPCell> cells, int indexCounter, List<DerivedField> derivedFields, DataField rawField) {
        PMMLFieldToVector featureEntries;
        OpType opType;
        if (derivedFields.size() == 0) {
            opType = rawField.getOpType();
        } else {
            opType = derivedFields.get(0).getOpType();
        }

        if (opType.value().equals("continuous")) {
            featureEntries = new PMMLFieldToVector.ContinousSingleEntryFieldToVector(rawField, derivedFields.toArray(new
                    DerivedField[derivedFields
                    .size()]));
        } else if (opType.value().equals("categorical")) {
            featureEntries = new PMMLFieldToVector.SparseCategorical1OfKFieldToVector(rawField, derivedFields.toArray(new
                    DerivedField[derivedFields
                    .size()]));
        } else {
            throw new UnsupportedOperationException("Only iplemented continuous and categorical variables so far.");
        }

        for (PPCell cell : cells) {
            featureEntries.addVectorEntry(indexCounter, cell);
            indexCounter++;
        }
        return featureEntries;
    }

    static PMMLFieldToVector getFeatureEntryFromGeneralRegressionModel(PMML model, int modelIndex, String fieldName, List<PPCell> cells, int indexCounter) {
        if (model.getModels().get(modelIndex) instanceof GeneralRegressionModel == false) {
            throw new UnsupportedOperationException("Can only do GeneralRegressionModel so far");
        }
        if (model.getModels().get(modelIndex).getLocalTransformations() != null) {
            throw new UnsupportedOperationException("Local transformations not implemented yet. ");
        }
        List<DerivedField> derivedFields = new ArrayList<>();
        String rawFieldName = ProcessPMMLHelper.getDerivedFields(fieldName, model.getTransformationDictionary(), derivedFields);
        DataField rawField = ProcessPMMLHelper.getRawDataField(model, rawFieldName);

        PMMLFieldToVector featureEntries;
        if (rawField == null) {
            throw new UnsupportedOperationException("Could not trace back {} to a raw input field. Maybe saomething is not implemented " +
                    "yet or the PMML file is faulty.");
        } else {
            featureEntries = getFieldVector(cells, indexCounter, derivedFields, rawField);
        }
        return featureEntries;

    }

    static List<FieldToVector> convertToFeatureEntries(PMML pmml, int modelNum, TreeMap<String, List<PPCell>> fieldToPPCellMap,
                                                       List<String> orderedParameterList) {
        // for each predictor: get vector entries?
        List<FieldToVector> fieldToVectorList = new ArrayList<>();
        int indexCounter = 0;
        // for each of the fields create the feature entries
        for (String fieldname : fieldToPPCellMap.keySet()) {
            PMMLFieldToVector featureEntries = getFeatureEntryFromGeneralRegressionModel(pmml, modelNum, fieldname,
                    fieldToPPCellMap.get(fieldname), indexCounter);
            for (PPCell cell : fieldToPPCellMap.get(fieldname)) {
                orderedParameterList.add(cell.getParameterName());
            }
            indexCounter += featureEntries.size();
            fieldToVectorList.add(featureEntries);

        }
        return fieldToVectorList;
    }

    static TreeMap<String, List<PPCell>> mapCellsToFields(GeneralRegressionModel grModel) {
        //get all the field names for multinomialLogistic model
        TreeMap<String, List<PPCell>> fieldToPPCellMap = new TreeMap<>();
        for (Predictor predictor : grModel.getFactorList().getPredictors()) {
            fieldToPPCellMap.put(predictor.getName().toString(), new ArrayList<PPCell>());
        }
        for (Predictor predictor : grModel.getCovariateList().getPredictors()) {
            fieldToPPCellMap.put(predictor.getName().toString(), new ArrayList<PPCell>());
        }

        // get all the entries and sort them by field.
        // then create one feature entry per feild and add them to features.
        // also we must keep a list of parameter names here to make sure the model uses the same order!

        for (PPCell ppcell : grModel.getPPMatrix().getPPCells()) {
            fieldToPPCellMap.get(ppcell.getField().toString()).add(ppcell);
        }
        return fieldToPPCellMap;
    }

    static void addIntercept(GeneralRegressionModel grModel, List<FieldToVector> fieldToVectorMap, Map<String, List<PPCell>>
            fieldToPPCellMap, List<String> orderedParameterList) {
        // now, find the order of vector entries to model parameters. This is extremely annoying but we have to do it at some
        // point...
        int numFeatures = 0; // current index?
        Set<String> allFieldParameters = new HashSet<>();
        for (Map.Entry<String, List<PPCell>> fieldAndCells : fieldToPPCellMap.entrySet()) {
            for (PPCell cell : fieldAndCells.getValue()) {
                allFieldParameters.add(cell.getParameterName());
                numFeatures++;
            }
        }
        // now find the parameters which do not come form a field
        for (Parameter parameter : grModel.getParameterList().getParameters()) {
            if (allFieldParameters.contains(parameter.getName()) == false) {
                PMMLFieldToVector.Intercept intercept = new PMMLFieldToVector.Intercept(parameter.getName());
                intercept.addVectorEntry(numFeatures, null);
                numFeatures++;
                fieldToVectorMap.add(intercept);
                orderedParameterList.add(parameter.getName());
            }
        }
    }

    static PMMLModelScriptEngineService.FeaturesAndModel getGeneralRegressionFeaturesAndModel(PMML pmml, int modelNum) {
        GeneralRegressionModel grModel = (GeneralRegressionModel) pmml.getModels().get(modelNum);
        if (grModel.getFunctionName().value().equals("classification") && grModel.getModelType().value().equals
                ("multinomialLogistic")) {
            TreeMap<String, List<PPCell>> fieldToPPCellMap = mapCellsToFields(grModel);
            List<String> orderedParameterList = new ArrayList<>();
            List<FieldToVector> fieldToVectorList = convertToFeatureEntries(pmml, modelNum, fieldToPPCellMap, orderedParameterList);
            //add intercept if any
            addIntercept(grModel, fieldToVectorList, fieldToPPCellMap, orderedParameterList);

            assert orderedParameterList.size() == grModel.getParameterList().getParameters().size();
            FieldsToVectorPMML vectorEntries = createGeneralizedRegressionModelVectorEntries(fieldToVectorList, orderedParameterList
                    .toArray(new String[orderedParameterList.size()]));

            // now finally create the model!


            return new PMMLModelScriptEngineService.FeaturesAndModel(vectorEntries, null);

        } else {
            throw new UnsupportedOperationException("Only implemented logistic regression with multinomialLogistic so far.");
        }
    }

    private static FieldsToVectorPMML createGeneralizedRegressionModelVectorEntries(List<FieldToVector>
                                                                                           fieldToVectorList, String[] orderedParameterList) {
        int numEntries = 0;
        for (FieldToVector entry : fieldToVectorList) {

            numEntries += entry.size();
        }
        return new FieldsToVectorPMML.FieldsToVectorPMMLGeneralizedRegression(fieldToVectorList, numEntries, orderedParameterList);
    }
}
