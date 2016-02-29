package org.elasticsearch.script;/*
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


import org.apache.lucene.index.Fields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.dmg.pmml.FieldName;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.io.IOException;
import java.util.*;


public class SharedMethods {
    public static Tuple<int[], double[]> getIndicesAndValuesFromTermVectors(List<Integer> indices, List<Integer> values, Fields fields, String field, Map<String, Integer> wordMap) throws IOException {
        Terms terms = fields.terms(field);
        TermsEnum termsEnum = terms.iterator();
        BytesRef t;
        PostingsEnum docsEnum = null;

        int numTerms = 0;
        indices.clear();
        values.clear();
        while ((t = termsEnum.next()) != null) {
            Integer termIndex = wordMap.get(t.utf8ToString());
            if (termIndex != null) {
                indices.add(termIndex);
                docsEnum = termsEnum.postings(docsEnum, PostingsEnum.FREQS);
                int nextDoc = docsEnum.nextDoc();
                assert nextDoc != PostingsEnum.NO_MORE_DOCS;
                values.add(docsEnum.freq());
                nextDoc = docsEnum.nextDoc();
                assert nextDoc == PostingsEnum.NO_MORE_DOCS;
                numTerms++;
            }
        }
        int[] indicesArray = new int[numTerms];
        double[] valuesArray = new double[numTerms];
        for (int i = 0; i < numTerms; i++) {
            indicesArray[i] = indices.get(i);
            valuesArray[i] = values.get(i);
        }
        return new Tuple<>(indicesArray, valuesArray);
    }

    static GetResponse getStoredParameters(Map<String, Object> params, Client client) {
        // get the stored parameters
        String index = (String) params.get("index");
        if (index == null) {
            throw new ScriptException("cannot initialize " + SVMModelScriptWithStoredParametersAndSparseVector.SCRIPT_NAME + ": parameter \"index\" missing");
        }
        String type = (String) params.get("type");
        if (index == null) {
            throw new ScriptException("cannot initialize " + SVMModelScriptWithStoredParametersAndSparseVector.SCRIPT_NAME + ": parameter \"type\" missing");
        }
        String id = (String) params.get("id");
        if (index == null) {
            throw new ScriptException("cannot initialize " + SVMModelScriptWithStoredParametersAndSparseVector.SCRIPT_NAME + ": parameter \"id\" missing");
        }
        GetResponse getResponse = client.prepareGet(index, type, id).get();
        if (getResponse.isExists() == false) {
            throw new ScriptException("cannot initialize " + SVMModelScriptWithStoredParametersAndSparseVector.SCRIPT_NAME + ": document " + index + "/" + type + "/" + id);
        }
        return getResponse;
    }

    static void fillWordIndexMap(ArrayList features, Map<String, Integer> wordMap) {
        for (int i = 0; i < features.size(); i++) {
            wordMap.put((String) features.get(i), i);
            if (i > 0) {
                if (((String) features.get(i)).compareTo(((String) features.get(i - 1))) < 0) {
                    throw new IllegalArgumentException("features must be sorted! these are in wrong order: " + features.get(i - 1) + " " + features.get(i));
                }
            }
        }
    }

    static EsLinearSVMModel initializeSVMModel(ArrayList features, String field, GetResponse getResponse) {
        ArrayList weightsArrayList = (ArrayList) getResponse.getSource().get("weights");
        double[] weights = new double[weightsArrayList.size()];
        for (int i = 0; i < weightsArrayList.size(); i++) {
            weights[i] = ((Number) weightsArrayList.get(i)).doubleValue();
        }
        Number intercept = (Number) getResponse.getSource().get("intercept");
        features.addAll((ArrayList) getResponse.getSource().get("features"));
        if (field == null || features == null || weightsArrayList == null || intercept == null) {
            throw new ScriptException("cannot initialize " + SVMModelScriptWithStoredParametersAndSparseVector.SCRIPT_NAME + ": one of the following parameters missing: field, features, weights, weights, intercept");
        }
        return new EsLinearSVMModel(weights, intercept.doubleValue(), new String[]{"0", "1"});
    }

    static EsLogisticRegressionModel initializeLRModel(ArrayList features, String field, GetResponse getResponse) {
        ArrayList weightsArrayList = (ArrayList) getResponse.getSource().get("weights");
        double[] weights = new double[weightsArrayList.size()];
        for (int i = 0; i < weightsArrayList.size(); i++) {
            weights[i] = ((Number) weightsArrayList.get(i)).doubleValue();
        }
        Number intercept = (Number) getResponse.getSource().get("intercept");
        features.addAll((ArrayList) getResponse.getSource().get("features"));
        if (field == null || features == null || weightsArrayList == null || intercept == null) {
            throw new ScriptException("cannot initialize " + SVMModelScriptWithStoredParametersAndSparseVector.SCRIPT_NAME + ": one of the following parameters missing: field, features, weights, weights, intercept");
        }
        return new EsLogisticRegressionModel(weights, intercept.doubleValue(), new String[]{"0", "1"});
    }

    static EsNaiveBayesModel initializeNaiveBayesModel(ArrayList features, String field, GetResponse getResponse) {
        ArrayList piAsArrayList = (ArrayList) getResponse.getSource().get("pi");
        ArrayList labelsAsArrayList = (ArrayList) getResponse.getSource().get("labels");
        ArrayList thetasAsArrayList = (ArrayList) getResponse.getSource().get("thetas");
        features.addAll((ArrayList) getResponse.getSource().get("features"));
        if (field == null || features == null || piAsArrayList == null || labelsAsArrayList == null || thetasAsArrayList == null) {
            throw new ScriptException("cannot initialize " + NaiveBayesModelScriptWithStoredParametersAndSparseVector.SCRIPT_NAME + ": one of the following parameters missing: field, features, pi, thetas, labels");
        }
        double[] pi = new double[piAsArrayList.size()];
        for (int i = 0; i < piAsArrayList.size(); i++) {
            pi[i] = ((Number) piAsArrayList.get(i)).doubleValue();
        }
        double[] labels = new double[labelsAsArrayList.size()];
        for (int i = 0; i < labelsAsArrayList.size(); i++) {
            labels[i] = ((Number) labelsAsArrayList.get(i)).doubleValue();
        }
        double thetas[][] = new double[labels.length][features.size()];
        for (int i = 0; i < thetasAsArrayList.size(); i++) {
            ArrayList thetaRow = (ArrayList) thetasAsArrayList.get(i);
            for (int j = 0; j < thetaRow.size(); j++) {
                thetas[i][j] = ((Number) thetaRow.get(j)).doubleValue();
            }
        }
        return new EsNaiveBayesModel(thetas, pi, new String[]{Double.toString(labels[0]), Double.toString(labels[0])});
    }

    static Tuple<int[], double[]> getIndicesAndValuesFromFielddataFields(Map<String, Integer> wordMap, ScriptDocValues<String> docValues) {
        Tuple<int[], double[]> indicesAndValues;
        List<Integer> indices = new ArrayList<>();

        for (String value : docValues.getValues()) {
            Integer index = wordMap.get(value);
            if (index != null) {
                indices.add(index);
            }
        }
        int[] indicesArray = new int[indices.size()];
        double[] valuesArray = new double[indices.size()];
        for (int i = 0; i < indices.size(); i++) {
            indicesArray[i] = indices.get(i).intValue();
            valuesArray[i] = 1;
        }
        indicesAndValues = new Tuple<>(indicesArray, valuesArray);
        return indicesAndValues;
    }

    static Tuple<int[], double[]> getIndicesAndValuesFromAnalyzedTokens(Map<String, Integer> wordMap, List<AnalyzeResponse.AnalyzeToken> tokens) {
        Tuple<int[], double[]> indicesAndValues;
        Map<Integer, Double> indicesAndValuesMap = new HashMap<>();

        for (AnalyzeResponse.AnalyzeToken value : tokens) {
            Integer index = wordMap.get(value.getTerm());
            if (index != null) {
                Double tf = indicesAndValuesMap.get(index);
                if (tf != null) {
                    indicesAndValuesMap.put(index, tf + 1.0);
                } else {
                    indicesAndValuesMap.put(index, 1.0);
                }
            }
        }
        int[] indicesArray = new int[indicesAndValuesMap.size()];
        double[] valuesArray = new double[indicesAndValuesMap.size()];
        SortedSet<Integer> keys = new TreeSet<>(indicesAndValuesMap.keySet());
        int i = 0;
        for (Integer key : keys) {
            Double value = indicesAndValuesMap.get(key);
            indicesArray[i] = key;
            valuesArray[i] = value;
            i++;
        }
        indicesAndValues = new Tuple<>(indicesArray, valuesArray);
        return indicesAndValues;
    }

    public static Map<FieldName, Double> getFieldNamesAndValuesFromFielddataFields(Map<String, Integer> wordMap, ScriptDocValues<String> docValues) {
        Map<FieldName, Double> fieldNamesAndValues = new HashMap<>();
        for (String value : docValues.getValues()) {
            Integer index = wordMap.get(value);
            if (index != null) {
                fieldNamesAndValues.put(new FieldName("field_" + index.toString()), new Double(1));
            }
        }
        return fieldNamesAndValues;
    }
}
