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

package org.elasticsearch.ml.modelinput;

import org.elasticsearch.action.preparespec.TransportPrepareSpecAction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class VectorRangesToVectorJSON extends VectorRangesToVector {


    // number of entries
    public VectorRangesToVectorJSON(Map<String, Object> source) {
        assert source.get("sparse") == null || source.get("sparse") instanceof Boolean;
        sparse = TransportPrepareSpecAction.getSparse(source.get("sparse"));
        assert (source.containsKey("features"));
        @SuppressWarnings("unchecked")
        ArrayList<Map<String, Object>> featuresArray = (ArrayList<Map<String, Object>>) source.get("features");
        int offset = 0;
        for (Map<String, Object> feature : featuresArray) {
            assert feature.get("field") != null;
            assert feature.get("type") != null;
            assert feature.get("type").equals("terms"); // nothing else implemented yet
            assert feature.get("terms") != null;
            assert feature.get("number") != null;
            if (sparse) {
                vectorRangeList.add(new AnalyzedTextVectorRange.SparseTermVectorRange((String) feature.get("field"), "int",
                        getTerms(feature.get("terms")),
                        (String) feature.get("number"),
                        offset));
            } else {
                vectorRangeList.add(new AnalyzedTextVectorRange.DenseTermVectorRange((String) feature.get("field"), "int", getTerms
                        (feature.get("terms")), (String) feature.get("number"), offset));
            }
            offset += vectorRangeList.get(vectorRangeList.size() - 1).size();
            numEntries += vectorRangeList.get(vectorRangeList.size() - 1).size();
        }
    }


    private String[] getTerms(Object terms) {
        assert terms instanceof ArrayList;
        @SuppressWarnings("unchecked") ArrayList<String> termsList = (ArrayList<String>) terms;
        String[] finalTerms = new String[termsList.size()];
        int i = 0;
        for (String term : termsList) {
            finalTerms[i] = term;
            i++;
        }
        return finalTerms;
    }

    public Object vector(DataSource dataSource) {
        if (sparse) {
            int length = 0;
            List<EsSparseNumericVector> entries = new ArrayList<>();
            for (VectorRange fieldEntry : vectorRangeList) {
                EsSparseNumericVector vec = (EsSparseNumericVector) fieldEntry.getVector(dataSource);
                entries.add(vec);
                length += vec.values.v1().length;
            }
            Map<String, Object> finalVector = new HashMap<>();


            double[] values = new double[length];
            int[] indices = new int[length];
            int curPos = 0;
            for (EsSparseNumericVector vector : entries) {
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
            for (VectorRange fieldEntry : vectorRangeList) {
                EsDenseNumericVector vec = (EsDenseNumericVector) fieldEntry.getVector(dataSource);
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

