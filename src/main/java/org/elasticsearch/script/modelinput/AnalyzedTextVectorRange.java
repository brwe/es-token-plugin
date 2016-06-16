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

import org.apache.lucene.index.Fields;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.SharedMethods;
import org.elasticsearch.search.lookup.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AnalyzedTextVectorRange extends VectorRange {
    int offset;

    public static final EsSparseNumericVector EMPTY_SPARSE = new EsSparseNumericVector(new Tuple<>(new int[]{}, new double[]{}));

    public AnalyzedTextVectorRange(String field, String type) {
        super(field, field, type);
    }
    public enum FeatureType {
        OCCURRENCE,
        TF,
        TF_IDF,
        BM25;

        public String toString() {
            switch (this.ordinal()) {
                case 0:
                    return "occurrence";
                case 1:
                    return "tf";
                case 2:
                    return "tf_idf";
                case 3:
                    return "bm25";
            }
            throw new IllegalStateException("There is no toString() for ordinal " + this.ordinal() + " - someone forgot to implement toString().");
        }

        public static FeatureType fromString(String s) {
            if (s.equals(OCCURRENCE.toString())) {
                return OCCURRENCE;
            } else if (s.equals(TF.toString())) {
                return TF;
            } else if (s.equals(TF_IDF.toString())) {
                return TF_IDF;
            } else if (s.equals(BM25.toString())) {
                return BM25;
            } else {
                throw new IllegalStateException("Don't know what " + s + " is - choose one of " + OCCURRENCE.toString() + " " + TF.toString() + " " + TF_IDF.toString() + " " + BM25.toString());
            }
        }
    }

    public static class SparseTermVectorRange extends AnalyzedTextVectorRange {
        private String number;
        Map<String, Integer> wordMap;

        public SparseTermVectorRange(String field, String type, String[] terms, String number, int offset) {
            super(field, type);
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
                if (AnalyzedTextVectorRange.FeatureType.fromString(number).equals(AnalyzedTextVectorRange.FeatureType.TF)) {
                    Fields fields = leafIndexLookup.termVectors();
                    if (fields == null) {
                        //ScriptDocValues<String> docValues = (ScriptDocValues.Strings) docLookup.get(field);
                        //indicesAndValues = SharedMethods.getIndicesAndTfsFromFielddataFieldsAndIndexLookup(categoryToIndexHashMap, docValues, leafIndexLookup.get(field));
                        return EMPTY_SPARSE;
                    } else {
                        indicesAndValues = SharedMethods.getIndicesAndValuesFromTermVectors(fields, field, wordMap);
                    }

                } else if (AnalyzedTextVectorRange.FeatureType.fromString(number).equals(AnalyzedTextVectorRange.FeatureType.OCCURRENCE)) {
                    ScriptDocValues<String> docValues = (ScriptDocValues.Strings) docLookup.get(field);
                    indicesAndValues = SharedMethods.getIndicesAndValuesFromFielddataFields(wordMap, docValues);
                } else if (AnalyzedTextVectorRange.FeatureType.fromString(number).equals(AnalyzedTextVectorRange.FeatureType.TF_IDF)) {
                    Fields fields = leafIndexLookup.termVectors();
                    if (fields == null) {
                        //ScriptDocValues<String> docValues = (ScriptDocValues.Strings) docLookup.get(field);
                        //ScriptDocValues<String> docValues = (ScriptDocValues.Strings) docLookup.get(field);
                        //indicesAndValues = SharedMethods.getIndicesAndTfsFromFielddataFieldsAndIndexLookup(categoryToIndexHashMap, docValues, leafIndexLookup.get(field));
                        return EMPTY_SPARSE;
                    } else {
                        indicesAndValues = SharedMethods.getIndicesAndTF_IDFFromTermVectors(fields, field, wordMap, leafIndexLookup);
                    }

                } else {
                    throw new ScriptException(number + " not implemented yet for sparse vector");
                }
                return new EsSparseNumericVector(indicesAndValues);
            } catch (IOException ex) {
                throw new ScriptException("Could not create sparse vector: ", ex);
            }
        }

        @Override
        public EsVector getVector(Map<String, List> fieldValues) {
            throw new UnsupportedOperationException("Remove this later, we should not get here.");
        }

        @Override
        public int size() {
            return wordMap.size();
        }
    }


    public static class DenseTermVectorRange extends AnalyzedTextVectorRange {
        String[] terms;
        String number;

        public DenseTermVectorRange(String field, String type, String[] terms, String number, int offset) {
            super(field, type);
            this.terms = terms;
            this.number = number;
            this.offset = offset;
            this.field = field;
        }

        @Override
        public EsVector getVector(LeafDocLookup docLookup, LeafFieldsLookup fieldsLookup, LeafIndexLookup leafIndexLookup) {
            double[] values = new double[terms.length];
            try {
                IndexField indexField = leafIndexLookup.get(field);
                for (int i = 0; i < terms.length; i++) {
                    IndexFieldTerm indexTermField = indexField.get(terms[i]);
                    if (AnalyzedTextVectorRange.FeatureType.fromString(number).equals(AnalyzedTextVectorRange.FeatureType.TF)) {
                        values[i] = indexTermField.tf();
                    } else if (AnalyzedTextVectorRange.FeatureType.fromString(number).equals(AnalyzedTextVectorRange.FeatureType.OCCURRENCE)) {
                        values[i] = indexTermField.tf() > 0 ? 1 : 0;
                    } else if (AnalyzedTextVectorRange.FeatureType.fromString(number).equals(AnalyzedTextVectorRange.FeatureType.TF_IDF)) {
                        double tf = indexTermField.tf();
                        double df = indexTermField.df();
                        double numDocs = indexField.docCount();
                        values[i] = tf * Math.log((numDocs + 1) / (df + 1));
                    } else {
                        throw new ScriptException(number + " not implemented yet for dense vector");
                    }
                }
                return new EsDenseNumericVector(values);
            } catch (IOException ex) {
                throw new ScriptException("Could not get tf vector: ", ex);
            }
        }

        @Override
        public EsVector getVector(Map<String, List> fieldValues) {
            throw new UnsupportedOperationException("Remove this later, we should not get here.");
        }

        @Override
        public int size() {
            return terms.length;
        }
    }
}
