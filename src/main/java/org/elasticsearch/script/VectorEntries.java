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
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafFieldsLookup;
import org.elasticsearch.search.lookup.LeafIndexLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class VectorEntries {

    public boolean isSparse() {
        return sparse;
    }

    boolean sparse;
    List<FeatureEntries> features = new ArrayList<>();

    int numEntries;

    public List<FeatureEntries> getEntries() {
        return features;
    }

    // number of entries
    public VectorEntries(Map<String, Object> source) {
        assert source.get("sparse") == null || source.get("sparse") instanceof Boolean;
        sparse = TransportPrepareSpecAction.getSparse(source.get("sparse"));
        assert (source.containsKey("features"));
        ArrayList<Map<String, Object>> featuresArray = (ArrayList<Map<String, Object>>) source.get("features");
        int offset = 0;
        for (Map<String, Object> feature : featuresArray) {
            assert feature.get("field") != null;
            assert feature.get("type") != null;
            assert feature.get("type").equals("terms"); // nothing else implemented yet
            assert feature.get("terms") != null;
            assert feature.get("number") != null;
            if (sparse) {
                features.add(new FeatureEntries.SparseTermFeatureEntries((String) feature.get("field"),
                        getTerms(feature.get("terms")),
                        (String) feature.get("number"),
                        offset));
            } else {
                features.add(new FeatureEntries.DenseTermFeatureEntries((String) feature.get("field"), getTerms(feature.get("terms")), (String) feature.get("number"), offset));
            }
            offset += features.get(features.size() - 1).size();
            numEntries += features.get(features.size() - 1).size();
        }
    }

    public VectorEntries(PMML pmml, int modelNum /* for which model find the vectors?**/) {
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
        // try to find field in local transform dictionary of model
        if (model.getModels().get(modelIndex).getLocalTransformations() != null) {
            throw new UnsupportedOperationException("Local transformations not implemented yet. ");
        }
        for (DerivedField derivedField : model.getTransformationDictionary().getDerivedFields()) {
            if (derivedField.getName().getValue().equals(fieldName)) {
                // at this point we need to find all possible references to this field backwards until we finally
                // arrive at a raw input field. for now we just implement simple one derived field per raw field.
                // throw uoe if the original field cannot be found.
                // try to find the field this derived field references.
                String rawField = null;
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
                        rawField = ((FieldRef) expression).getField().getValue();
                    }

                }
                if (rawField == null) {
                    throw new UnsupportedOperationException("could not find raw field name. Maybe this derived field references another derived field? Did not implement that yet.");
                }
                // now find the actual dataField
                for (DataField dataField : model.getDataDictionary().getDataFields()) {
                    String rawDataFieldName = dataField.getName().getValue();
                    if (rawDataFieldName.equals(fieldName)) {
                        return createVectorEntries(dataField, derivedField);
                    }
                }

            }


        }

        // try to find field in transform dictionary
        // try to find field in dictionary
        throw new UnsupportedOperationException();

    }

    private FeatureEntries createVectorEntries(DataField dataField, DerivedField derivedField) {
        if (dataField.getOpType().value().equals("categorical")) {
            List<String> categories = new ArrayList<>();
            for (Value value : dataField.getValues()) {
                categories.add(value.getValue());
            }
            if (derivedField != null) {

            }
            throw new UnsupportedOperationException("have not implemented any derived field for continous variables yet. I am working" +
                    " as quick as I can! At least when I feel like it...which is not often lately...seriously, need more holiday...");
        } else if (dataField.getOpType().value().equals("continuous")) {
            if (derivedField != null) {
                throw new UnsupportedOperationException("have not implemented any derived field for continous variables yet. I am working" +
                        " as quick as I can! At least when I feel like it...which is not often lately...seriously, need more holiday...");
            }
            return null; //new FeatureEntries.ContinousSingleEntryFeatureEntries(dataField.getName().getValue(), offset);
        } else {
            throw new UnsupportedOperationException("have not implemented any field except for continuous and categorical yet. " +
                    dataField.getOpType().value() + " is not supported. I am working" +
                    " as quick as I can! At least when I feel like it...which is not often lately...seriously, need more holiday...");
        }
    }

    private String[] getTerms(Object terms) {
        assert terms instanceof ArrayList;
        ArrayList<String> termsList = (ArrayList<String>) terms;
        String[] finalTerms = new String[termsList.size()];
        int i = 0;
        for (String term : termsList) {
            finalTerms[i] = term;
            i++;
        }
        return finalTerms;
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

