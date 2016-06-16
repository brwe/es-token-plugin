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

import org.dmg.pmml.Apply;
import org.dmg.pmml.Constant;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.Expression;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.NormContinuous;
import org.dmg.pmml.PPCell;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafFieldsLookup;
import org.elasticsearch.search.lookup.LeafIndexLookup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
* Maps a single field to vector entries. Includes pre processing.
* */
public abstract class PMMLVectorRange extends VectorRange {

    protected PreProcessingStep[] preProcessingSteps;


    protected Object applyPreProcessing(Map<String, List> fieldValues) {
        List valueList = fieldValues.get(field);
        Object value = valueList == null || valueList.size() == 0 ? null : valueList.get(0);
        for (int i = 0; i < preProcessingSteps.length; i++) {
            value = preProcessingSteps[i].apply(value);
        }
        return value;
    }

    public PMMLVectorRange(DataField dataField, MiningField miningField, DerivedField[] derivedFields) {
        super(dataField.getName().getValue(),
                derivedFields.length == 0 ? dataField.getName().getValue() : derivedFields[derivedFields.length - 1].getName().getValue(),
                derivedFields.length == 0 ? dataField.getDataType().value() : derivedFields[derivedFields.length - 1].getDataType().value());
        this.field = dataField.getName().getValue();
        if (miningField.getMissingValueReplacement() != null) {
            preProcessingSteps = new PreProcessingStep[derivedFields.length + 1];
            preProcessingSteps[0] = new MissingValuePreProcess(dataField, miningField.getMissingValueReplacement());
        } else {
            preProcessingSteps = new PreProcessingStep[derivedFields.length];
        }
        fillPreProcessingSteps(derivedFields);
    }
    public PMMLVectorRange(String field, String lastDerivedFieldName, String type) {
        super(field, lastDerivedFieldName, type);
    }

    public abstract void addVectorEntry(int indexCounter, PPCell ppcell);

    /**
     * Converts a 1 of k feature into a vector that has a 1 where the field value is the nth category and 0 everywhere else.
     * Categories will be numbered according to the order given in categories parameter.
     */
    public static class SparseCategorical1OfKVectorRange extends PMMLVectorRange {
        Map<String, Integer> categoryToIndexHashMap = new HashMap<>();

        public SparseCategorical1OfKVectorRange(DataField dataField, MiningField miningField, DerivedField[] derivedFields) {
            super(dataField, miningField, derivedFields);
        }

        @Override
        public EsVector getVector(LeafDocLookup docLookup, LeafFieldsLookup fieldsLookup, LeafIndexLookup leafIndexLookup) {
            throw new UnsupportedOperationException("Remove this later, we should not get here.");
        }

        @Override
        public EsVector getVector(Map<String, List> fieldValues) {
            Tuple<int[], double[]> indicesAndValues;
            Object processedCategory = applyPreProcessing(fieldValues);
            Integer index = categoryToIndexHashMap.get(processedCategory);
            if (index == null) {
                // TODO: Should we throw an exception here? Can this actually happen?
                return new EsSparseNumericVector(new Tuple<>(new int[]{}, new double[]{}));
            } else {
                indicesAndValues = new Tuple<>(new int[]{index}, new double[]{1.0});
                return new EsSparseNumericVector(indicesAndValues);
            }
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
    public static class ContinousSingleEntryVectorRange extends PMMLVectorRange {
        int index = -1;

        /**
         * The derived fields must be given in backwards order of the processing chain.
         */
        public ContinousSingleEntryVectorRange(DataField dataField, MiningField miningField, DerivedField... derivedFields) {
            super(dataField, miningField, derivedFields);

        }

        @Override
        public EsVector getVector(LeafDocLookup docLookup, LeafFieldsLookup fieldsLookup, LeafIndexLookup leafIndexLookup) {
            throw new UnsupportedOperationException("Remove this later, we should not get here.");
        }

        @Override
        public EsVector getVector(Map<String, List> fieldValues) {
            Tuple<int[], double[]> indicesAndValues;
            Object finalValue = applyPreProcessing(fieldValues);
            indicesAndValues = new Tuple<>(new int[]{index}, new double[]{((Number) finalValue).doubleValue()});
            return new EsSparseNumericVector(indicesAndValues);
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

    protected void fillPreProcessingSteps(DerivedField[] derivedFields) {

        int derivedFieldIndex = derivedFields.length - 1;
        // don't start at the beginning, we might have a pre processing step there already from the mining field
        for (int preProcessingStepIndex = preProcessingSteps.length - derivedFields.length; preProcessingStepIndex < preProcessingSteps.length;
             preProcessingStepIndex++) {
            DerivedField derivedField = derivedFields[derivedFieldIndex];
            if (derivedField.getExpression() != null) {
                handleExpression(preProcessingStepIndex, derivedField);

            } else {
                throw new UnsupportedOperationException("So far only Apply implemented.");
            }
            derivedFieldIndex--;
        }
    }

    private void handleExpression(int preProcessingStepIndex, DerivedField derivedField) {
        if (derivedField.getExpression() instanceof Apply) {
            for (Expression expression : ((Apply) derivedField.getExpression()).getExpressions()) {
                if (expression instanceof Apply) {
                    if (((Apply) expression).getFunction().equals("isMissing")) {
                        // now find the value that is supposed to replace the missing value

                        for (Expression expression2 : ((Apply) derivedField.getExpression()).getExpressions()) {
                            if (expression2 instanceof Constant) {
                                String missingValue = ((Constant) expression2).getValue();
                                preProcessingSteps[preProcessingStepIndex] = new MissingValuePreProcess(derivedField, missingValue);
                                break;
                            }
                        }
                    } else {
                        throw new UnsupportedOperationException("So far only if isMissing implemented.");
                    }
                }
            }
        } else if (derivedField.getExpression() instanceof NormContinuous) {
            preProcessingSteps[preProcessingStepIndex] = new NormContinousPreProcess((NormContinuous) derivedField
                    .getExpression(), derivedField.getName().getValue());
        } else {
            throw new UnsupportedOperationException("So far only Apply expression implemented.");
        }
    }

    public static class Intercept extends PMMLVectorRange {
        int index;
        private String interceptName;

        public Intercept(String interceptName, String type) {
            super(null, null, type);
            this.interceptName = interceptName;
        }

        @Override
        public void addVectorEntry(int indexCounter, PPCell ppcell) {
            this.index = indexCounter;
        }

        @Override
        public int size() {
            return 1;
        }

        @Override
        public EsVector getVector(LeafDocLookup docLookup, LeafFieldsLookup fieldsLookup, LeafIndexLookup leafIndexLookup) {
            return new EsSparseNumericVector(new Tuple<>(new int[]{index}, new double[]{1.0}));
        }

        @Override
        public EsVector getVector(Map<String, List> fieldValues) {
            return new EsSparseNumericVector(new Tuple<>(new int[]{index}, new double[]{1.0}));
        }
    }

    public static class FieldToValue extends PMMLVectorRange {
        String finalFieldName;

        public FieldToValue(DataField dataField, MiningField miningField, DerivedField... derivedFields) {
            super(dataField, miningField, derivedFields);
            finalFieldName = preProcessingSteps.length > 0 ? preProcessingSteps[preProcessingSteps.length - 1].name() : field;
        }

        @Override
        public void addVectorEntry(int indexCounter, PPCell ppcell) {
            throw new UnsupportedOperationException("Not implemented for FieldToValue");
        }

        @Override
        public int size() {
            return 1;
        }

        @Override
        public EsVector getVector(LeafDocLookup docLookup, LeafFieldsLookup fieldsLookup, LeafIndexLookup leafIndexLookup) {
            throw new UnsupportedOperationException("Not implemented for FieldToValue");
        }

        @Override
        public EsVector getVector(Map<String, List> fieldValues) {
            Object finalValue = applyPreProcessing(fieldValues);
            Map<String, Object> values = new HashMap<>();
            values.put(finalFieldName, finalValue);
            return new EsValueMapVector(values);
        }
    }
}
