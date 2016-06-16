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

package org.elasticsearch.script.modelinput;

import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafFieldsLookup;
import org.elasticsearch.search.lookup.LeafIndexLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class VectorRangesToVectorPMML extends VectorRangesToVector {

    public VectorRangesToVectorPMML(List<VectorRange> fieldsToVectors, int numEntries) {
        this.sparse = true;
        this.vectorRangeList = fieldsToVectors;
        this.numEntries = numEntries;
    }

    public Object vector(LeafDocLookup docLookup, LeafFieldsLookup fieldsLookup, LeafIndexLookup leafIndexLookup, SourceLookup sourceLookup) {

        HashMap<String, List> fieldValues = new HashMap<>();
        for (VectorRange vectorRange : this.vectorRangeList) {
            // TODO: vector range can depend on several fields
            String field = vectorRange.getField();
            if (field != null) {
                // TODO: We assume here doc lookup will always give us something back. What if not?
                fieldValues.put(field, ((ScriptDocValues) docLookup.get(field)).getValues());
            }
        }
        return vector(fieldValues);
    }

    public Object vector(Map<String, List> fieldValues) {
        int length = 0;
        List<EsSparseNumericVector> sparseNumericVectors = new ArrayList<>();
        for (VectorRange vectorRange : this.vectorRangeList) {
            EsVector entries = vectorRange.getVector(fieldValues);
            assert entries instanceof EsSparseNumericVector;
            sparseNumericVectors.add((EsSparseNumericVector) entries);
            length += ((EsSparseNumericVector) entries).values.v1().length;
        }
        Map<String, Object> finalVector = new HashMap<>();
        double[] values = new double[length];
        int[] indices = new int[length];
        int i = 0;
        for (EsSparseNumericVector esSparseNumericVector : sparseNumericVectors) {
            for (int j = 0; j < esSparseNumericVector.values.v1().length; j++) {
                indices[i] = esSparseNumericVector.values.v1()[j];
                values[i] = esSparseNumericVector.values.v2()[j];
                i++;
            }
        }
        finalVector.put("values", values);
        finalVector.put("indices", indices);
        finalVector.put("length", numEntries);
        return finalVector;
    }

    public static class VectorRangesToVectorPMMLGeneralizedRegression extends VectorRangesToVectorPMML {

        public String[] getOrderedParameterList() {
            return orderedParameterList;
        }

        private final String[] orderedParameterList;

        public VectorRangesToVectorPMMLGeneralizedRegression(List<VectorRange> features, int numEntries, String[] orderedParameterList) {
            super(features, numEntries);
            this.orderedParameterList = orderedParameterList;
        }
    }

    public static class VectorRangesToVectorPMMLTreeModel extends VectorRangesToVectorPMML {

        public VectorRangesToVectorPMMLTreeModel(List<VectorRange> fieldsToVectors) {
            super(fieldsToVectors, fieldsToVectors.size());
        }

        @Override
        public Object vector(Map<String, List> fieldValues) {
            HashMap<String, Object> values = new HashMap<>();
            for (VectorRange vectorRange : vectorRangeList) {
                assert vectorRange instanceof PMMLVectorRange.FieldToValue;
                values.putAll(((EsValueMapVector) vectorRange.getVector(fieldValues)).getValues());
            }
            return values;
        }
    }
}

