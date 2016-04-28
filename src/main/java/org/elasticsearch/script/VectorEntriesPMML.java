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

import org.dmg.pmml.*;
import org.elasticsearch.action.preparespec.TransportPrepareSpecAction;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafFieldsLookup;
import org.elasticsearch.search.lookup.LeafIndexLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class VectorEntriesPMML extends VectorEntries {

    public VectorEntriesPMML(PMML pmml, int modelNum /* for which model find the vectors?**/) {
        Model model = pmml.getModels().get(modelNum);
        if (model instanceof GeneralRegressionModel == false) {
            throw new UnsupportedOperationException("Only implemented general regression model so far.");
        }
        GeneralRegressionModel grModel = (GeneralRegressionModel) model;
        if (grModel.getAlgorithmName().equals("LogisticRegression") == false || grModel.getModelType().value().equals
                ("multinomialLogistic") == false) {
            throw new UnsupportedOperationException("Only implemented logistic regression with multinomialLogistic so far.");
        }
        //get all the field names for multinomialLogistic model
        List<String> usedFields = new ArrayList<>();
        for (Predictor predictor : grModel.getFactorList().getPredictors()) {
            usedFields.add(predictor.getName().getValue());
        }
        for (Predictor predictor : grModel.getCovariateList().getPredictors()) {
            usedFields.add(predictor.getName().getValue());
        }
        // for each predictor: get vector entries?
        Map<String, FeatureEntries> featureEntriesMap = new HashMap();
        int indexCounter = 0;
        for (PPCell ppcell : grModel.getPPMatrix().getPPCells()) {
            String fieldName = ppcell.getField().getValue();
            if (featureEntriesMap.containsKey(ppcell.getField().getValue()) == false) {
                featureEntriesMap.put(ppcell.getField().getValue(), getFeatureEntryFromModel(pmml, modelNum, fieldName));
            }
            FeatureEntries featureEntries = featureEntriesMap.get(fieldName);
            featureEntries.addVectorEntry(indexCounter, ppcell);
            indexCounter++;
        }

        // TODO: now remember the featureEntries and find different abstraction for that stuff to a proper sparse vector
    }

    FeatureEntries getFeatureEntryFromModel(PMML model, int modelIndex, String fieldName) {
        /*We need to find the path from field to actual raw input field. To do so, we start with the field name that the model expects
        and then trace back to the original fields via local transformations, tranfomration dictionary and finally data dictionary. */
        // try to find field in local transform dictionary of model
        if (model.getModels().get(modelIndex).getLocalTransformations() != null) {
            throw new UnsupportedOperationException("Local transformations not implemented yet. ");
        }
        // trace back all derived fields until we must arrive at an actual data field. This unfortunately means we have to
        // loop over dervied fields as often as we find one..
        DerivedField lastFoundDerivedField;
        String lastFieldName = fieldName;
        List <DerivedField> derivedFields = new ArrayList<>();
        do {
            lastFoundDerivedField = null;
            for (DerivedField derivedField : model.getTransformationDictionary().getDerivedFields()) {
                if (derivedField.getName().getValue().equals(lastFieldName)) {
                    lastFoundDerivedField = derivedField;
                    derivedFields.add(derivedField);
                    // now get the next fieldname this field references
                    // this is tricky, because this information can be anywhere...
                    lastFieldName = getReferencedFieldName(derivedField);
                    lastFoundDerivedField = derivedField;
                }
            }
        } while (lastFoundDerivedField != null);

        // now find the actual dataField
        DataField rawField = null;
        for (DataField dataField : model.getDataDictionary().getDataFields()) {
            String rawDataFieldName = dataField.getName().getValue();
            if (rawDataFieldName.equals(lastFieldName)) {
                rawField = dataField;
                break;
            }
        }
        if (rawField == null) {
            throw new UnsupportedOperationException("Could not trace back {} to a raw input field. Maybe saomething is not implemented " +
                    "yet or the PMML file is faulty.");
        }
        OpType opType;
        if (derivedFields.size() == 0) {
            opType = rawField.getOpType();
        } else {
            opType = derivedFields.get(0).getOpType();
        }
        if (opType.value().equals("continuous")) {
            return new FeatureEntries.ContinousSingleEntryFeatureEntries(rawField, derivedFields.toArray(new DerivedField[derivedFields
                    .size()]));
        } else if (opType.value().equals("categorical")) {
            return new FeatureEntries.SparseCategorical1OfKFeatureEntries(rawField, derivedFields.toArray(new DerivedField[derivedFields
                    .size()]));
        } else {
            throw new UnsupportedOperationException("Only iplemented continuous and categorical variables so far.");
        }

    }

    private String getReferencedFieldName(DerivedField derivedField) {
        String referencedField = null;
        if (derivedField.getExpression() == null) {
            // there is a million ways in which derived fields can reference other fields.
            // need to implement them all!
            throw new UnsupportedOperationException("So far only implemented if function for derived fields.");
        }
        if (derivedField.getExpression() instanceof Apply == false) {
            throw new UnsupportedOperationException("So far only Apply expression implemented.");
        }
        // TODO throw uoe in case the function is not "if missing" - much more to implement!
        for (Expression expression : ((Apply) derivedField.getExpression()).getExpressions()) {
            if (expression instanceof FieldRef) {
                referencedField = ((FieldRef) expression).getField().getValue();
            }

        }
        if (referencedField == null) {
            throw new UnsupportedOperationException("could not find raw field name. Maybe this derived field references another derived field? Did not implement that yet.");
        }
        return referencedField;
    }

    public Object vector(LeafDocLookup docLookup, LeafFieldsLookup fieldsLookup, LeafIndexLookup leafIndexLookup, SourceLookup sourceLookup) {
        if (sparse) {
            int length = 0;
            List<EsSparseVector> entries = new ArrayList<>();
            for (FeatureEntries fieldEntry : features) {
                EsSparseVector vec = (EsSparseVector) fieldEntry.getVector(docLookup, fieldsLookup, leafIndexLookup);
                entries.add(vec);
                length += vec.values.v1().length;
            }
            Map<String, Object> finalVector = new HashMap<>();


            double[] values = new double[length];
            int[] indices = new int[length];
            int curPos = 0;
            for (EsSparseVector vector : entries) {
                int numValues = vector.values.v1().length;
                System.arraycopy(vector.values.v1(), 0, indices, curPos, numValues);
                System.arraycopy(vector.values.v2(), 0, values, curPos, numValues);
                curPos += numValues;
            }
            finalVector.put("values", values);
            finalVector.put("indices", indices);
            finalVector.put("length", numEntries);
            return finalVector;

        } else {
            int length = 0;
            List<double[]> entries = new ArrayList<>();
            for (FeatureEntries fieldEntry : features) {
                EsDenseVector vec = (EsDenseVector) fieldEntry.getVector(docLookup, fieldsLookup, leafIndexLookup);
                entries.add(vec.values);
                length += vec.values.length;
            }
            Map<String, Object> finalVector = new HashMap<>();
            double[] values = new double[length];
            int curPos = 0;
            for (double[] vals : entries) {
                int numValues = vals.length;
                System.arraycopy(vals, 0, values, curPos, numValues);
                curPos += numValues;
            }
            finalVector.put("values", values);
            finalVector.put("length", numEntries);
            return finalVector;
        }
    }
}

