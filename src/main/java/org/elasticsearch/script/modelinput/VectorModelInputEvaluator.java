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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Converts data source to a sparse vector data input
 */
public class VectorModelInputEvaluator implements ModelInputEvaluator<VectorModelInput>  {

    private final List<VectorRange> vectorRangeList;

    public VectorModelInputEvaluator(List<VectorRange> vectorRangeList) {
        this.vectorRangeList = vectorRangeList;
    }

    public List<VectorRange> getVectorRangeList() {
        return vectorRangeList;
    }

    @Override
    public SparseVectorModelInput convert(DataSource dataSource) {
        // TODO: Optimize!!!
        HashMap<String, List<Object>> fieldValues = new HashMap<>();
        for (VectorRange vectorRange : this.vectorRangeList) {
            // TODO: vector range can depend on several fields
            String field = vectorRange.getField();
            if (field != null) {
                fieldValues.put(field, dataSource.getValues(field));
            }
        }
        int length = 0;
        List<EsSparseNumericVector> sparseNumericVectors = new ArrayList<>();
        for (VectorRange vectorRange : this.vectorRangeList) {
            EsVector entries = vectorRange.getVector(fieldValues);
            assert entries instanceof EsSparseNumericVector;
            sparseNumericVectors.add((EsSparseNumericVector) entries);
            length += ((EsSparseNumericVector) entries).values.v1().length;
        }
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
        return new SparseVectorModelInput(values, indices);
    }

}
