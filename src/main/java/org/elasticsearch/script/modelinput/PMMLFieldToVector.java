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
import org.dmg.pmml.DataType;
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
public abstract class PMMLFieldToVector extends FieldToVector {

    protected PreProcessingStep[] preProcessingSteps;


    protected Object applyPreProcessing(Object value) {
        for (int i = 0; i < preProcessingSteps.length; i++) {
            value = preProcessingSteps[i].apply(value);
        }
        return value;
    }


    public abstract void addVectorEntry(int indexCounter, PPCell ppcell);

    /**
     * Converts a 1 of k feature into a vector that has a 1 where the field value is the nth category and 0 everywhere else.
     * Categories will be numbered according to the order given in categories parameter.
     */
    public static class SparseCategorical1OfKFieldToVector extends PMMLFieldToVector {
        Map<String, Integer> categoryToIndexHashMap = new HashMap<>();

        public SparseCategorical1OfKFieldToVector(DataField dataField, MiningField miningField, DerivedField[] derivedFields) {
            this.field = dataField.getName().getValue();
            if (miningField.getMissingValueReplacement() != null) {
                preProcessingSteps = new PreProcessingStep[derivedFields.length + 1];
                preProcessingSteps[0] = new MissingValuePreProcess(miningField.getMissingValueReplacement());
            } else {
                preProcessingSteps = new PreProcessingStep[derivedFields.length];
            }
            fillPreProcessingSteps(derivedFields);
        }

        @Override
        public EsVector getVector(LeafDocLookup docLookup, LeafFieldsLookup fieldsLookup, LeafIndexLookup leafIndexLookup) {
            throw new UnsupportedOperationException("Remove this later, we should not get here.");
        }

        @Override
        public EsVector getVector(Map<String, List> fieldValues) {
            Tuple<int[], double[]> indicesAndValues;
            List category = fieldValues.get(field);
            Object processedCategory = applyPreProcessing(category == null || category.size() == 0 ? null : category.get(0));
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
    public static class ContinousSingleEntryFieldToVector extends PMMLFieldToVector {
        int index = -1;

        /**
         * The derived fields must be given in backwards order of the processing chain.
         */
        public ContinousSingleEntryFieldToVector(DataField dataField, MiningField miningField, DerivedField... derivedFields) {
            this.field = dataField.getName().getValue();
            if (miningField.getMissingValueReplacement() != null) {
                preProcessingSteps = new PreProcessingStep[derivedFields.length + 1];
                preProcessingSteps[0] = new MissingValuePreProcess(miningField.getMissingValueReplacement());
            } else {
                preProcessingSteps = new PreProcessingStep[derivedFields.length];
            }
            fillPreProcessingSteps(derivedFields);

        }

        @Override
        public EsVector getVector(LeafDocLookup docLookup, LeafFieldsLookup fieldsLookup, LeafIndexLookup leafIndexLookup) {
            throw new UnsupportedOperationException("Remove this later, we should not get here.");
        }

        @Override
        public EsVector getVector(Map<String, List> fieldValues) {
            Tuple<int[], double[]> indicesAndValues;
            List value = fieldValues.get(field);
            Object finalValue = applyPreProcessing(value.size() == 0 ? null : value.get(0));
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
                                preProcessingSteps[preProcessingStepIndex] = new MissingValuePreProcess(parsedMissingValue);
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
                    .getExpression());
        } else {
            throw new UnsupportedOperationException("So far only Apply expression implemented.");
        }
    }

    public static class Intercept extends PMMLFieldToVector {
        int index;
        private String interceptName;

        public Intercept(String interceptName) {
            super();
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
}
