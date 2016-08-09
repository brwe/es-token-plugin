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

package org.elasticsearch.script.models;

import org.elasticsearch.common.collect.Tuple;

import java.util.Map;


public abstract class EsNumericInputModelEvaluator extends EsModelEvaluator<MapModelInput> {

    abstract Map<String, Object> evaluateDebug(Tuple<int[], double[]> featureValues);

    abstract Object evaluate(Tuple<int[], double[]> featureValues);

    abstract Map<String, Object> evaluateDebug(double[] featureValues);

    @Override
    public Map<String, Object> evaluateDebug(MapModelInput modelInput) {
        Map<String, Object> vector = modelInput.getAsMap();
        if (vector.containsKey("indices") == false) {
            Map<String, Object> denseVector = vector;
            assert (denseVector.get("values") instanceof double[]);
            return evaluateDebug((double[]) denseVector.get("values"));
        } else {
            Map<String, Object> sparseVector = vector;
            assert (sparseVector.get("indices") instanceof int[]);
            assert (sparseVector.get("values") instanceof double[]);
            Tuple<int[], double[]> indicesAndValues = new Tuple<>((int[]) sparseVector.get("indices"), (double[]) sparseVector.get
                    ("values"));
            return evaluateDebug(indicesAndValues);
        }
    }

    @Override
    public Object evaluate(MapModelInput modelInput) {
        Map<String, Object> vector = modelInput.getAsMap();
        if (vector.containsKey("indices") == false) {
         throw new UnsupportedOperationException("cannot evaluate dense vector without param debug: true");
        }
        Map<String, Object> sparseVector = vector;
        assert (sparseVector.get("indices") instanceof int[]);
        assert (sparseVector.get("values") instanceof double[]);
        Tuple<int[], double[]> indicesAndValues = new Tuple<>((int[]) sparseVector.get("indices"), (double[]) sparseVector.get("values"));
        return evaluate(indicesAndValues);

    }
}


