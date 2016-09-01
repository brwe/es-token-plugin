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

import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Represents an abstract data source that could provide information about a single record.
 */
public interface DataSource {
    /**
     * Returns a list of values for the given field
     */
    <T> List<T> getValues(String field);

    /**
     * Returns an array of 0s and 1s. 1 if the corresponding term in the terms array is present in the field and 0 otherwise.
     */
    double[] getOccurrenceDense(String[] terms, String field);

    /**
     * Returns an array of TF/IDF values for the terms in the specified field
     */
    double[] getTfIdfDense(String[] terms, String field);

    /**
     * Returns an array of TF values for the terms in the specified field
     */
    double[] getTfDense(String[] terms, String field);

    /**
     * Returns a sparse array of 0s and 1s. 1 if the corresponding term in the wordMap is present in the field and 0 otherwise.
     */
    default Tuple<int[], double[]> getOccurrenceSparse(Map<String, Integer> wordMap, String field) {
        List<String> docValues = getValues(field);
        Tuple<int[], double[]> indicesAndValues;
        List<Integer> indices = new ArrayList<>();

        for (String value : docValues) {
            Integer index = wordMap.get(value);
            if (index != null) {
                indices.add(index);
            }
        }
        int[] indicesArray = new int[indices.size()];
        double[] valuesArray = new double[indices.size()];
        for (int i = 0; i < indices.size(); i++) {
            indicesArray[i] = indices.get(i);
            valuesArray[i] = 1;
        }
        indicesAndValues = new Tuple<>(indicesArray, valuesArray);
        return indicesAndValues;
    }

    /**
     * Returns a sparse array of TF/IDF values for the terms in the specified field
     */
    Tuple<int[], double[]> getTfIdfSparse(Map<String, Integer> wordMap, String field);

    /**
     * Returns a sparse array of TF values for the terms in the specified field
     */
    Tuple<int[], double[]> getTfSparse(Map<String, Integer> wordMap, String field);

}
