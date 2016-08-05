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

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.script.modelinput.DataSource;

import java.util.List;
import java.util.Map;

/**
 * Test implementation of a datasource
 */
public class MockDataSource implements DataSource {

    private final Map<String, List<Object>> data;

    public MockDataSource(Map<String, List<Object>> data) {
        this.data = data;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> List<T> getValues(String field) {
        return (List<T>) data.get(field);
    }

    @Override
    public double[] getOccurrenceDense(String[] terms, String field) {
        double[] values = new double[terms.length];
        List<Object> fieldValues = data.get(field);
        for (int i = 0; i < terms.length; i++) {
            for (Object fieldValue : fieldValues) {
                if (terms[i].equals(fieldValue)) {
                    values[i] = 1;
                }
            }
        }
        return values;
    }

    @Override
    public double[] getTfIdfDense(String[] terms, String field) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public double[] getTfDense(String[] terms, String field) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public Tuple<int[], double[]> getTfIdfSparse(Map<String, Integer> wordMap, String field) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public Tuple<int[], double[]> getTfSparse(Map<String, Integer> wordMap, String field) {
        throw new UnsupportedOperationException("not implemented yet");
    }
}
