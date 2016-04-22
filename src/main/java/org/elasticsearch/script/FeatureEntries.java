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
import org.dmg.pmml.*;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class FeatureEntries {
    int offset;
    String field;

    protected PreProcessingStep[] preProcessingSteps;

    public static final EsSparseVector EMPTY_SPARSE = new EsSparseVector(new Tuple<>(new int[]{}, new double[]{}));

    protected Object applyPreProcessing(Object value) {
        for (int i = 0; i < preProcessingSteps.length; i++) {
            value = preProcessingSteps[i].apply(value);
        }
        return value;
    }

    public static class MissingValue implements PreProcessingStep {

        private Object missingValue;

        public MissingValue(Object missingValue) {
            this.missingValue = missingValue;
        }

        @Override
        public Object apply(Object value) {
            if (value == null) {
                return missingValue;
            } else {
                return value;
            }
        }
    }

    public abstract void addVectorEntry(int indexCounter, PPCell ppcell);

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

    public abstract int size();

    public abstract EsVector getVector(LeafDocLookup docLookup, LeafFieldsLookup fieldsLookup, LeafIndexLookup leafIndexLookup);

    public static class SparseTermFeatureEntries extends FeatureEntries {
        private String number;
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
                if (FeatureType.fromString(number).equals(FeatureType.TF)) {
                    Fields fields = leafIndexLookup.termVectors();
                    if (fields == null) {
                        //ScriptDocValues<String> docValues = (ScriptDocValues.Strings) docLookup.get(field);
                        //indicesAndValues = SharedMethods.getIndicesAndTfsFromFielddataFieldsAndIndexLookup(categoryToIndexHashMap, docValues, leafIndexLookup.get(field));
                        return EMPTY_SPARSE;
                    } else {
                        indicesAndValues = SharedMethods.getIndicesAndValuesFromTermVectors(fields, field, wordMap);
                    }

                } else if (FeatureType.fromString(number).equals(FeatureType.OCCURRENCE)) {
                    ScriptDocValues<String> docValues = (ScriptDocValues.Strings) docLookup.get(field);
                    indicesAndValues = SharedMethods.getIndicesAndValuesFromFielddataFields(wordMap, docValues);
                } else if (FeatureType.fromString(number).equals(FeatureType.TF_IDF)) {
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
                return new EsSparseVector(indicesAndValues);
            } catch (IOException ex) {
                throw new ScriptException("Could not create sparse vector: ", ex);
            }
        }

        @Override
        public void addVectorEntry(int indexCounter, PPCell ppcell) {
            throw new UnsupportedOperationException("not available for term vector entries");
        }

        @Override
        public int size() {
            return wordMap.size();
        }


    }

    /**
     * Converts a 1 of k feature into a vector that has a 1 where the field value is the nth category and 0 everywhere else.
     * Categories will be numbered according to the order given in categories parameter.
     */
    public static class SparseCategorical1OfKFeatureEntries extends FeatureEntries {
        Map<String, Integer> categoryToIndexHashMap = new HashMap<>();

        public SparseCategorical1OfKFeatureEntries(DataField dataField, DerivedField[] derivedFields) {
            this.field = dataField.getName().getValue();
            preProcessingSteps = new PreProcessingStep[derivedFields.length];
            fillPreProcessingSteps(derivedFields);
        }

        @Override
        public EsVector getVector(LeafDocLookup docLookup, LeafFieldsLookup fieldsLookup, LeafIndexLookup leafIndexLookup) {
            Tuple<int[], double[]> indicesAndValues;
            Object category = docLookup.get(field);
            Object processedCategory = applyPreProcessing(category);
            int index = categoryToIndexHashMap.get(processedCategory);
            indicesAndValues = new Tuple<>(new int[]{index}, new double[]{1.0});
            return new EsSparseVector(indicesAndValues);
        }

        @Override
        public void addVectorEntry(int indexCounter, PPCell ppcell) {
            categoryToIndexHashMap.put(ppcell.getValue(), indexCounter);
        }

        @Override
        public int size() {
            return categoryToIndexHashMap.size();
        }

    }

    /**
     * Converts a 1 of k feature into a vector that has a 1 where the field value is the nth category and 0 everywhere else.
     * Categories will be numbered according to the order given in categories parameter.
     */
    public static class ContinousSingleEntryFeatureEntries extends FeatureEntries {
        int index = -1;

        /**
         * The derived fields must be given in backwards order of the processing chain.
         */
        public ContinousSingleEntryFeatureEntries(DataField dataField, DerivedField... derivedFields) {
            this.field = dataField.getName().getValue();
            preProcessingSteps = new PreProcessingStep[derivedFields.length];
            fillPreProcessingSteps(derivedFields);

        }

        @Override
        public EsVector getVector(LeafDocLookup docLookup, LeafFieldsLookup fieldsLookup, LeafIndexLookup leafIndexLookup) {
            Tuple<int[], double[]> indicesAndValues;
            Object value = docLookup.get(field);
            value = applyPreProcessing(value);
            indicesAndValues = new Tuple<>(new int[]{index}, new double[]{((Number) value).doubleValue()});
            return new EsSparseVector(indicesAndValues);
        }

        @Override
        public void addVectorEntry(int indexCounter, PPCell ppcell) {
            index = indexCounter;
        }

        @Override
        public int size() {
            return 1;
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
            double[] values = new double[terms.length];
            try {
                IndexField indexField = leafIndexLookup.get(field);
                for (int i = 0; i < terms.length; i++) {
                    IndexFieldTerm indexTermField = indexField.get(terms[i]);
                    if (FeatureType.fromString(number).equals(FeatureType.TF)) {
                        values[i] = indexTermField.tf();
                    } else if (FeatureType.fromString(number).equals(FeatureType.OCCURRENCE)) {
                        values[i] = indexTermField.tf() > 0 ? 1 : 0;
                    } else if (FeatureType.fromString(number).equals(FeatureType.TF_IDF)) {
                        double tf = indexTermField.tf();
                        double df = indexTermField.df();
                        double numDocs = indexField.docCount();
                        values[i] = tf * Math.log((numDocs + 1) / (df + 1));
                    } else {
                        throw new ScriptException(number + " not implemented yet for dense vector");
                    }
                }
                return new EsDenseVector(values);
            } catch (IOException ex) {
                throw new ScriptException("Could not get tf vector: ", ex);
            }
        }

        @Override
        public void addVectorEntry(int indexCounter, PPCell ppcell) {
            throw new UnsupportedOperationException("not available for term vector entries");
        }

        @Override
        public int size() {
            return terms.length;
        }
    }

    protected void fillPreProcessingSteps(DerivedField[] derivedFields) {
        for (int i = derivedFields.length - 1; i >= 0; i--) {
            DerivedField derivedField = derivedFields[i];
            if (derivedField.getExpression() != null) {
                if (derivedField.getExpression() instanceof Apply) {
                    for (Expression expression : ((Apply) derivedField.getExpression()).getExpressions()) {
                        if (expression instanceof Apply) {
                            if (((Apply) expression).getFunction().equals("isMissing")) {
                                // now find the value that is supposed to replace the missing value

                                for (Expression expression2 : ((Apply) derivedField.getExpression()).getExpressions()) {
                                    if (expression2 instanceof Constant) {
                                        String missingValue = ((Constant) expression2).getValue();
                                        Object parsedMissingValue;
                                        if (derivedField.getDataType().equals(DataType.DOUBLE)) {
                                            parsedMissingValue = Double.parseDouble(missingValue);
                                        } else if (derivedField.getDataType().equals(DataType.FLOAT)) {
                                            parsedMissingValue = Float.parseFloat(missingValue);
                                        } else if (derivedField.getDataType().equals(DataType.INTEGER)) {
                                            parsedMissingValue = Integer.parseInt(missingValue);
                                        } else if (derivedField.getDataType().equals(DataType.STRING)) {
                                            parsedMissingValue = missingValue;
                                        } else {
                                            throw new UnsupportedOperationException("Only implemented data type double, float and int so " +
                                                    "far.");
                                        }
                                        preProcessingSteps[i] = new MissingValue(parsedMissingValue);
                                        break;
                                    }
                                }
                            } else {
                                throw new UnsupportedOperationException("So far only if isMissing implemented.");
                            }
                        }
                    }
                } else {
                    throw new UnsupportedOperationException("So far only Apply expression implemented.");
                }
            } else {
                throw new UnsupportedOperationException("So far only Apply implemented.");
            }
        }
    }
}
