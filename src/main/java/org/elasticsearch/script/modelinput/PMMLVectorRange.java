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
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
* Maps a single field to vector entries. Includes pre processing.
* */
public abstract class PMMLVectorRange extends VectorRange {

    protected PreProcessingStep[] preProcessingSteps;


    protected List<Object> applyPreProcessing(Map<String, List<Object>> fieldValues) {
        List<Object> processedValues = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();

        if (fieldValues.get(field) == null) {
            valueList = new ArrayList<>();
            valueList.add(null);
        } else if (fieldValues.get(field).size() == 0) {
            valueList.add(null);
        } else {
            valueList.addAll(fieldValues.get(field));
        }
        for (Object value : valueList) {
            for (int i = 0; i < preProcessingSteps.length; i++) {
                value = preProcessingSteps[i].apply(value);
            }
            processedValues.add(value);
        }
        return processedValues;
    }

    public PMMLVectorRange(DataField dataField, MiningField miningField, DerivedField[] derivedFields) {
        super(dataField.getName().getValue(),
                derivedFields.length == 0 ? dataField.getName().getValue() : derivedFields[derivedFields.length - 1].getName().getValue(),
                derivedFields.length == 0 ? dataField.getDataType().value() : derivedFields[derivedFields.length - 1].getDataType().value
                        ());
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

    public abstract void addVectorEntry(int indexCounter, String value);

    /**
     * Converts a 1 of k feature into a vector that has a 1 where the field value is the nth category and 0 everywhere else.
     * Categories will be numbered according to the order given in categories parameter.
     */
    public static class SparseCategoricalVectorRange extends PMMLVectorRange {
        Map<String, Integer> categoryToIndexHashMap = new HashMap<>();

        public SparseCategoricalVectorRange(DataField dataField, MiningField miningField, DerivedField[] derivedFields) {
            super(dataField, miningField, derivedFields);
        }

        @Override
        public EsVector getVector(DataSource dataSource) {
            throw new UnsupportedOperationException("Remove this later, we should not get here.");
        }

        @Override
        public EsVector getVector(Map<String, List<Object>> fieldValues) {
            Tuple<int[], double[]> indicesAndValues;
            List<Object> processedCategory = applyPreProcessing(fieldValues);

            List<Integer> indices = new ArrayList<>();
            Integer lastIndex = -1;
            for (Object value : processedCategory) {
                Integer index = categoryToIndexHashMap.get(value);
                if (index != null) {
                    indices.add(index);
                    assert lastIndex < index;
                    lastIndex = index;
                }
            }
            int[] indicesArray = new int[indices.size()];
            double[] values = new double[indices.size()];
            int indexCounter = 0;
            for (Integer index : indices) {
                indicesArray[indexCounter] = index;
                values[indexCounter] = 1.0;
                indexCounter++;
            }
            indicesAndValues = new Tuple<>(indicesArray, values);
            return new EsSparseNumericVector(indicesAndValues);
        }

        @Override
        public void addVectorEntry(int indexCounter, String value) {
            categoryToIndexHashMap.put(value, indexCounter);
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
        public EsVector getVector(DataSource dataSource) {
            throw new UnsupportedOperationException("Remove this later, we should not get here.");
        }

        @Override
        public EsVector getVector(Map<String, List<Object>> fieldValues) {
            Tuple<int[], double[]> indicesAndValues;
            List<Object> finalValues = applyPreProcessing(fieldValues);
            if (finalValues.size() > 0) {
                indicesAndValues = new Tuple<>(new int[]{index}, new double[]{((Number) finalValues.get(0)).doubleValue()});
                return new EsSparseNumericVector(indicesAndValues);
            } else {
                return new EsSparseNumericVector(new Tuple<>(new int[]{}, new double[]{}));
            }
        }

        @Override
        public void addVectorEntry(int indexCounter, String value) {
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
        for (int preProcessingStepIndex = preProcessingSteps.length - derivedFields.length; preProcessingStepIndex < preProcessingSteps
                .length;
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
        public void addVectorEntry(int indexCounter, String value) {
            this.index = indexCounter;
        }

        @Override
        public int size() {
            return 1;
        }

        @Override
        public EsVector getVector(DataSource dataSource) {
            return new EsSparseNumericVector(new Tuple<>(new int[]{index}, new double[]{1.0}));
        }

        @Override
        public EsVector getVector(Map<String, List<Object>> fieldValues) {
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
        public void addVectorEntry(int indexCounter, String value) {
            throw new UnsupportedOperationException("Not implemented for FieldToValue");
        }

        @Override
        public int size() {
            return 1;
        }

        @Override
        public EsVector getVector(DataSource dataSource) {
            throw new UnsupportedOperationException("Not implemented for FieldToValue");
        }

        @Override
        public EsVector getVector(Map<String, List<Object>> fieldValues) {
            List<Object> finalValue = applyPreProcessing(fieldValues);
            Set<Object> valueSet = new HashSet<>();
            valueSet.addAll(finalValue);
            Map<String, Set<Object>> values = new HashMap<>();
            values.put(finalFieldName, valueSet);
            return new EsValueMapVector(values);
        }
    }
}
