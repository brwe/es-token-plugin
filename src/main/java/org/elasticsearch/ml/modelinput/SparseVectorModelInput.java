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

import java.util.HashMap;
import java.util.Map;

/**
 * A sparse vector implementation of the vector model
 */
public class SparseVectorModelInput extends VectorModelInput {

    private final double[] values;
    private final int[] indices;
    private Map<String, Object> map;

    public SparseVectorModelInput(double[] values, int[] indices) {
        this.values = values;
        this.indices = indices;
    }

    @Override
    public int getSize() {
        return values.length;
    }

    @Override
    public double getValue(int i) {
        return values[i];
    }

    @Override
    public int getIndex(int i) {
        return indices[i];
    }

    public double[] getValues() {
        return values;
    }

    public int[] getIndices() {
        return indices;
    }
}
