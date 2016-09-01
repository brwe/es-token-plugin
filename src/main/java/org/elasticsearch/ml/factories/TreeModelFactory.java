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

package org.elasticsearch.ml.factories;

import org.dmg.pmml.Array;
import org.dmg.pmml.CompoundPredicate;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.False;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.Node;
import org.dmg.pmml.Predicate;
import org.dmg.pmml.SimplePredicate;
import org.dmg.pmml.SimpleSetPredicate;
import org.dmg.pmml.TransformationDictionary;
import org.dmg.pmml.TreeModel;
import org.dmg.pmml.True;
import org.elasticsearch.ml.modelinput.VectorRange;
import org.elasticsearch.ml.modelinput.VectorRangesToVectorPMML;
import org.elasticsearch.ml.modelinput.PMMLVectorRange;
import org.elasticsearch.ml.models.EsTreeModel;
import org.elasticsearch.ml.modelinput.MapModelInput;
import org.elasticsearch.ml.modelinput.ModelAndModelInputEvaluator;
import org.elasticsearch.script.pmml.ProcessPMMLHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TreeModelFactory extends ModelFactory<MapModelInput, String, TreeModel> {

    public TreeModelFactory() {
        super(TreeModel.class);
    }

    @Override
    public ModelAndModelInputEvaluator<MapModelInput, String> buildFromPMML(TreeModel treeModel, DataDictionary dataDictionary,
                                                                    TransformationDictionary transformationDictionary) {
        if (treeModel.getFunctionName().value().equals("classification")
                && treeModel.getSplitCharacteristic().value().equals("binarySplit")
                && treeModel.getMissingValueStrategy().value().equals("defaultChild")
                && treeModel.getNoTrueChildStrategy().value().equals("returnLastPrediction")) {

            List<VectorRange> fields = getFieldValuesList(treeModel, dataDictionary, transformationDictionary);
            VectorRangesToVectorPMML.VectorRangesToVectorPMMLTreeModel fieldsToVector =
                    new VectorRangesToVectorPMML.VectorRangesToVectorPMMLTreeModel(fields);
            Map<String, String> fieldToTypeMap = getFieldToTypeMap(fields);
            EsTreeModel esTreeModel = getEsTreeModel(treeModel, fieldToTypeMap);
            return new ModelAndModelInputEvaluator<>(fieldsToVector, esTreeModel);
        } else {
            throw new UnsupportedOperationException("TreeModel does not support the following parameters yet: "
                    + " splitCharacteristic:" + treeModel.getSplitCharacteristic().value()
                    + " missingValueStrategy:" + treeModel.getMissingValueStrategy().value()
                    + " noTrueChildStrategy:" + treeModel.getNoTrueChildStrategy().value());
        }
    }

    protected static List<VectorRange> getFieldValuesList(TreeModel treeModel, DataDictionary dataDictionary,
                                                          TransformationDictionary transformationDictionary) {
        // walk the tree model and gather all the field name
        Set<String> fieldNames = new HashSet<>();
        Node startNode = treeModel.getNode();
        getFieldNamesFromNode(fieldNames, startNode);
        // create the actual VectorRange objects, copy paste much from GLMHelper
        List<VectorRange> fieldsToValues = new ArrayList<>();
        List<DerivedField> allDerivedFields = ProcessPMMLHelper.getAllDerivedFields(treeModel, transformationDictionary);
        for(String fieldName : fieldNames) {
            List<DerivedField> derivedFields = new ArrayList<>();
            String rawFieldName = ProcessPMMLHelper.getDerivedFields(fieldName, allDerivedFields, derivedFields);
            DataField rawField = ProcessPMMLHelper.getRawDataField(dataDictionary, rawFieldName);
            MiningField miningField = ProcessPMMLHelper.getMiningField(treeModel, rawFieldName);
            fieldsToValues.add(new PMMLVectorRange.FieldToValue(rawField, miningField, derivedFields.toArray(new
                    DerivedField[derivedFields.size()])));
        }
        return fieldsToValues;
    }

    protected static void getFieldNamesFromNode(Set<String> fieldNames, Node startNode) {
        Predicate predicate = startNode.getPredicate();
        getFieldNamesFromPredicate(fieldNames, predicate);
        for (Node node : startNode.getNodes()) {
            getFieldNamesFromNode(fieldNames, node);
        }
    }

    protected static void getFieldNamesFromPredicate(Set<String> fieldNames, Predicate predicate) {
        if (predicate instanceof CompoundPredicate) {
            List<Predicate> predicates = ((CompoundPredicate) predicate).getPredicates();
            for (Predicate predicate1 : predicates) {
                getFieldNamesFromPredicate(fieldNames, predicate1);
            }
        } else {
            if (predicate instanceof SimplePredicate) {
                fieldNames.add(((SimplePredicate) predicate).getField().getValue());
            } else if (predicate instanceof SimpleSetPredicate) {
                fieldNames.add(((SimpleSetPredicate) predicate).getField().getValue());
            }
        }
    }

    protected EsTreeModel getEsTreeModel(TreeModel treeModel, Map<String, String> fieldToTypeMap) {
        return new EsTreeModel(convertToEsTreeNode(treeModel.getNode(), fieldToTypeMap));
    }

    public static Map<String,String> getFieldToTypeMap(java.util.List<VectorRange> vectorRangeList) {
        Map<String, String> fieldToTypeMap = new HashMap<>();
        for (VectorRange vectorRange : vectorRangeList) {
            fieldToTypeMap.put(vectorRange.getLastDerivedFieldName(), vectorRange.getType());
        }
        return fieldToTypeMap;
    }


    private EsTreeModel.EsTreeNode convertToEsTreeNode(Node node, Map<String, String> fieldTypeMap) {
        List<EsTreeModel.EsTreeNode> childNodes = new ArrayList<>();
        EsTreeModel.EsPredicate predicate = createPredicate(node.getPredicate(), fieldTypeMap);
        for (Node childNode : node.getNodes()) {
            childNodes.add(convertToEsTreeNode(childNode, fieldTypeMap));
        }
        return new EsTreeModel.EsTreeNode(Collections.unmodifiableList(childNodes), predicate, node.getScore());
    }


    private static EsTreeModel.EsPredicate createPredicate(final Predicate predicate, Map<String, String> fieldTypeMap) {
        if (predicate instanceof SimplePredicate) {
            SimplePredicate simplePredicate = (SimplePredicate) predicate;
            String field = simplePredicate.getField().getValue();
            String type = fieldTypeMap.get(field);
            String value = simplePredicate.getValue();
            if (type == "string") {
                return getSimplePredicate(value, field, simplePredicate.getOperator().value());
            }
            if (type == "double") {
                return getSimplePredicate(Double.parseDouble(value), field, simplePredicate.getOperator().value());
            }
            if (type == "float") {
                return getSimplePredicate(Float.parseFloat(value), field, simplePredicate.getOperator().value());
            }
            if (type == "int") {
                return getSimplePredicate(Integer.parseInt(value), field, simplePredicate.getOperator().value());
            }
            if (type == "boolean") {
                return getSimplePredicate(Boolean.parseBoolean(value), field, simplePredicate.getOperator().value());
            }
            throw new UnsupportedOperationException("Data type " + type + " for TreeModel not implemented yet.");
        }
        if (predicate instanceof True) {
            return new EsTreeModel.EsPredicate() {
                @Override
                public boolean match(Map<String, Object> vector) {
                    return true;
                }

                @Override
                public boolean notEnoughValues(Map<String, Object> vector) {
                    return false;
                }
            };

        }
        if (predicate instanceof False) {
            return new EsTreeModel.EsPredicate() {
                @Override
                public boolean match(Map<String, Object> vector) {
                    return false;
                }

                @Override
                public boolean notEnoughValues(Map<String, Object> vector) {
                    return false;
                }
            };
        }
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            List<EsTreeModel.EsPredicate> predicates = new ArrayList<>();
            for (Predicate childPredicate : ((CompoundPredicate) predicate).getPredicates()) {
                predicates.add(createPredicate(childPredicate, fieldTypeMap));
            }
            if (compoundPredicate.getBooleanOperator().value().equals("and")) {
                return new EsTreeModel.EsCompoundPredicate(predicates) {

                    @Override
                    protected boolean matchList(Map<String, Object> vector) {
                        boolean result = true;
                        for (EsTreeModel.EsPredicate childPredicate : predicates) {
                            result = result && childPredicate.match(vector);
                        }
                        return result;
                    }
                };
            }
            if (compoundPredicate.getBooleanOperator().value().equals("or")) {
                return new EsTreeModel.EsCompoundPredicate(predicates) {

                    @Override
                    protected boolean matchList(Map<String, Object> vector) {
                        boolean result = false;
                        for (EsTreeModel.EsPredicate childPredicate : predicates) {
                            result = result || childPredicate.match(vector);
                        }
                        return result;
                    }
                };
            }
            if (compoundPredicate.getBooleanOperator().value().equals("xor")) {
                return new EsTreeModel.EsCompoundPredicate(predicates) {

                    @Override
                    protected boolean matchList(Map<String, Object> vector) {
                        boolean result = false;
                        for (EsTreeModel.EsPredicate childPredicate : predicates) {
                            if (result == false) {
                                result = result || childPredicate.match(vector);
                            } else {
                                if (childPredicate.match(vector)) {
                                    // we had true already, xor must return false
                                    return false;
                                }
                            }
                        }
                        return result;
                    }
                };
            }
            if (compoundPredicate.getBooleanOperator().value().equals("surrogate")) {
                return new EsTreeModel.EsCompoundPredicate(predicates) {

                    @Override
                    protected boolean matchList(Map<String, Object> vector) {
                        for (EsTreeModel.EsPredicate childPredicate : predicates) {
                            if (childPredicate.notEnoughValues(vector) == false) {
                                return childPredicate.match(vector);

                            }
                        }
                        return false;
                    }

                    @Override
                    public boolean notEnoughValues(Map<String, Object> vector) {
                        boolean notEnoughValues = true;
                        for (EsTreeModel.EsPredicate predicate : predicates) {
                            // only one needs to have enough values and then the predicate is defined
                            notEnoughValues = predicate.notEnoughValues(vector) && notEnoughValues;
                        }
                        return notEnoughValues;
                    }
                };

            }
        }
        if (predicate instanceof SimpleSetPredicate) {
            SimpleSetPredicate simpleSetPredicate = (SimpleSetPredicate) predicate;
            Array setArray = simpleSetPredicate.getArray();
            String field = simpleSetPredicate.getField().getValue();
            if (setArray.getType().equals(Array.Type.STRING)) {
                HashSet<String> valuesSet = new HashSet<>();
                String[] values = setArray.getValue().split("\" \"");
                // trimm beginning and end quotes
                values[0] = values[0].substring(1, values[0].length());
                values[values.length - 1] = values[values.length - 1].substring(0, values[values.length - 1].length() - 1);
                if (values.length != setArray.getN()) {
                    throw new UnsupportedOperationException("Could not infer values from array value " + setArray.getValue());
                }
                for (String value : values) {
                    valuesSet.add(value);
                }
                return new EsTreeModel.EsSimpleSetPredicate<>(valuesSet, field);
            }

            if (setArray.getType().equals(Array.Type.STRING)) {
                HashSet<Double> valuesSet = new HashSet<>();
                String[] values = setArray.getValue().split(" ");
                if (values.length != setArray.getN()) {
                    throw new UnsupportedOperationException("Could not infer values from array value " + setArray.getValue());
                }
                for (String value : values) {
                    valuesSet.add(Double.parseDouble(value));
                }
                return new EsTreeModel.EsSimpleSetPredicate<>(valuesSet, field);
            }
            if (setArray.getType().equals(Array.Type.INT)) {
                HashSet<Integer> valuesSet = new HashSet<>();
                String[] values = setArray.getValue().split(" ");
                if (values.length != setArray.getN()) {
                    throw new UnsupportedOperationException("Could not infer values from array value " + setArray.getValue());
                }
                for (String value : values) {
                    valuesSet.add(Integer.parseInt(value));
                }
                return new EsTreeModel.EsSimpleSetPredicate<>(valuesSet, field);
            }
        }
        throw new UnsupportedOperationException("Predicate Type " + predicate.getClass().getName() + " for TreeModel not implemented yet.");
    }

    protected static <T extends Comparable<T>> EsTreeModel.EsSimplePredicate<T> getSimplePredicate(T value, String field, String operator) {
        if (operator.equals("equal")) {
            return new EsTreeModel.EsSimplePredicate<T>(value, field) {
                @Override
                public boolean match(T fieldValue) {
                    return value.equals(fieldValue);
                }
            };
        }
        if (operator.equals("notEqual")) {
            return new EsTreeModel.EsSimplePredicate<T>(value, field) {
                @Override
                public boolean match(T fieldValue) {
                    return value.equals(fieldValue) == false;
                }
            };
        }
        if (operator.equals("lessThan")) {
            return new EsTreeModel.EsSimplePredicate<T>(value, field) {
                @Override
                public boolean match(T fieldValue) {
                    return fieldValue.compareTo(value) < 0;
                }
            };
        }
        if (operator.equals("lessOrEqual")) {
            return new EsTreeModel.EsSimplePredicate<T>(value, field) {
                @Override
                public boolean match(T fieldValue) {
                    return fieldValue.compareTo(value) <= 0;
                }
            };
        }
        if (operator.equals("greaterThan")) {
            return new EsTreeModel.EsSimplePredicate<T>(value, field) {
                @Override
                public boolean match(T fieldValue) {
                    return fieldValue.compareTo(value) > 0;
                }
            };
        }
        if (operator.equals("greaterOrEqual")) {
            return new EsTreeModel.EsSimplePredicate<T>(value, field) {
                @Override
                public boolean match(T fieldValue) {
                    return fieldValue.compareTo(value) >= 0;
                }
            };
        }
        if (operator.equals("isMissing")) {
            return new EsTreeModel.EsSimplePredicate<T>(value, field) {
                @Override
                public boolean match(T fieldValue) {
                    throw new UnsupportedOperationException("We should never get here!");
                }

                @Override
                public boolean match(Map<String, Object> vector) {
                    Object fieldValue = vector.get(field);
                    if (fieldValue == null) {
                        return true;
                    }
                    return false;
                }
            };
        }
        if (operator.equals("isNotMissing")) {
            return new EsTreeModel.EsSimplePredicate<T>(value, field) {
                @Override
                public boolean match(T fieldValue) {
                    throw new UnsupportedOperationException("We should never get here!");
                }

                @Override
                public boolean match(Map<String, Object> vector) {
                    Object fieldValue = vector.get(field);
                    if (fieldValue == null) {
                        return false;
                    }
                    return true;
                }
            };
        }
        throw new UnsupportedOperationException("OOperator " + operator + "  not supported for Predicate in TreeModel.");
    }
}
