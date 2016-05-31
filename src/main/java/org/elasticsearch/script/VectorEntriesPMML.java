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

import org.dmg.pmml.Apply;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.Expression;
import org.dmg.pmml.FieldRef;
import org.dmg.pmml.GeneralRegressionModel;
import org.dmg.pmml.Model;
import org.dmg.pmml.NormContinuous;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.PPCell;
import org.dmg.pmml.Predictor;
import org.dmg.pmml.TransformationDictionary;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafFieldsLookup;
import org.elasticsearch.search.lookup.LeafIndexLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


public class VectorEntriesPMML extends VectorEntries {

    public VectorEntriesPMML(List<FeatureEntries> features, int numEntries) {
        this.features = features;
        this.numEntries = numEntries;
    }

    public Object vector(LeafDocLookup docLookup, LeafFieldsLookup fieldsLookup, LeafIndexLookup leafIndexLookup, SourceLookup sourceLookup) {

        HashMap<String, List> fieldValues = new HashMap<>();
        for (FeatureEntries featureEntries : features) {
            String field = featureEntries.getField();
            // TODO: We assume here doc lookup will always give us something back. What if not?
            fieldValues.put(field, ((ScriptDocValues) docLookup.get(field)).getValues());
        }
        return vector(fieldValues);
    }

    protected Object vector(Map<String, List> fieldValues) {
        Map<Integer, Double> indicesAndValues = new TreeMap<>();
        for (FeatureEntries featureEntries : features) {
            EsVector entries = featureEntries.getVector(fieldValues);
            assert entries instanceof EsSparseVector;
            EsSparseVector sparseVector = (EsSparseVector) entries;
            for (int i = 0; i < sparseVector.values.v1().length; i++) {
                assert indicesAndValues.containsKey(sparseVector.values.v1()[i]) == false;
                indicesAndValues.put(sparseVector.values.v1()[i], sparseVector.values.v2()[i]);
            }
        }
        Map<String, Object> finalVector = new HashMap<>();
        double[] values = new double[indicesAndValues.size()];
        int[] indices = new int[indicesAndValues.size()];
        int i = 0;
        for (Map.Entry<Integer, Double> entry : indicesAndValues.entrySet()) {
            indices[i] = entry.getKey();
            values[i] = entry.getValue();
            i++;
        }
        finalVector.put("values", values);
        finalVector.put("indices", indices);
        finalVector.put("length", numEntries);
        return finalVector;
    }
}

