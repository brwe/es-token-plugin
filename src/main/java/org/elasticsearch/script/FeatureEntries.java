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

import org.apache.lucene.index.Fields;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class FeatureEntries {
    int offset;
    String field;

    public abstract int size();

    public abstract EsVector getVector(LeafDocLookup docLookup, LeafFieldsLookup fieldsLookup, LeafIndexLookup leafIndexLookup);

    public static class SparseTermFeatureEntries extends FeatureEntries {
        private String number;
        List<Integer> indices = new ArrayList<>();
        List<Integer> values = new ArrayList<>();
        Map<String, Integer> wordMap;

        public SparseTermFeatureEntries(String field, String[] terms, String number, int offset) {
            this.number = number;
            this.field = field;
            wordMap = new HashMap<>();
            for (int i = 0; i < terms.length; i++) {
                wordMap.put(terms[i], i + offset);
            }
        }

        @Override
        public EsVector getVector(LeafDocLookup docLookup, LeafFieldsLookup fieldsLookup, LeafIndexLookup leafIndexLookup) {
            try {
                /** here be the vectorizer **/
                Tuple<int[], double[]> indicesAndValues;
                if (number.equals("tf")) {
                    Fields fields = leafIndexLookup.termVectors();
                    if (fields == null) {
                        throw new ScriptException("Term vectors not stored! (We could do it without but Britta has not implemented it yet)");
                    }
                    indicesAndValues = SharedMethods.getIndicesAndValuesFromTermVectors(indices, values, fields, field, wordMap);

                } else {
                    ScriptDocValues<String> docValues = (ScriptDocValues.Strings) docLookup.get(field);
                    indicesAndValues = SharedMethods.getIndicesAndValuesFromFielddataFields(wordMap, docValues);
                }
                return new EsSparseVector(indicesAndValues);
            } catch (IOException ex) {
                throw new ScriptException("Could not create sparse vector: ", ex);
            }
        }

        @Override
        public int size() {
            return wordMap.size();
        }


    }

    public static class DenseTermFeatureEntries extends FeatureEntries {
        String[] terms;
        String number;

        public DenseTermFeatureEntries(String field, String[] terms, String number, int offset) {
            this.terms = terms;
            this.number = number;
            this.offset = offset;
            this.field = field;
        }

        @Override
        public EsVector getVector(LeafDocLookup docLookup, LeafFieldsLookup fieldsLookup, LeafIndexLookup leafIndexLookup) {
            double[] tfs = new double[terms.length];
            try {
                if (number.equals("tf")) {
                    IndexField indexField = leafIndexLookup.get(field);
                    for (int i = 0; i < terms.length; i++) {
                        IndexFieldTerm indexTermField = indexField.get(terms[i]);
                        tfs[i] = indexTermField.tf();
                    }

                } else {
                    throw new ScriptException("sense vector woth number: occurrence not implemented yet");
                }

                return new EsDenseVector(tfs);
            } catch (IOException ex) {
                throw new ScriptException("Could not get tf vector: ", ex);
            }
        }

        @Override
        public int size() {
            return terms.length;
        }
    }
}
