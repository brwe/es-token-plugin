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

import org.dmg.pmml.Array;
import org.dmg.pmml.CompoundPredicate;
import org.dmg.pmml.False;
import org.dmg.pmml.Node;
import org.dmg.pmml.Predicate;
import org.dmg.pmml.SimplePredicate;
import org.dmg.pmml.SimpleSetPredicate;
import org.dmg.pmml.TreeModel;
import org.dmg.pmml.True;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EsTreeModel extends EsModelEvaluator {

    EsTreeNode startNode;

    public EsTreeModel(TreeModel treeModel, Map<String, String> fieldTypeMap) {

        startNode = new EsTreeNode(treeModel.getNode(), fieldTypeMap);
    }

    @Override
    public Map<String, Object> evaluateDebug(Map<String, Object> vector) {
        assert startNode.predicate.match(vector);
        return startNode.evaluate(vector);
    }

    @Override
    public Object evaluate(Map<String, Object> vector) {
        assert startNode.predicate.match(vector);
        return startNode.evaluate(vector).get("class");
    }

    static class EsTreeNode {
        EsPredicate predicate;
        java.util.List<EsTreeNode> childNodes = new ArrayList<>();
        String score;

        public EsTreeNode(Node node, Map<String, String> fieldTypeMap) {
            predicate = createPredicate(node.getPredicate(), fieldTypeMap);
            for (Node childNode : node.getNodes()) {
                childNodes.add(new EsTreeNode(childNode, fieldTypeMap));
            }
            score = node.getScore();
        }

        public Map<String, Object> evaluate(Map<String, Object> vector) {
            for (EsTreeNode childNode : childNodes) {
                if (childNode.predicate.match(vector)) {
                    return childNode.evaluate(vector);
                }
            }
            Map<String, Object> result = new HashMap<>();
            result.put("class", score);
            return result;
        }
    }

    protected static EsPredicate createPredicate(final Predicate predicate, Map<String, String> fieldTypeMap) {
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
            return new EsPredicate() {
                @Override
                public boolean match(Map<String, Object> vector) {
                    return true;
                }

                @Override
                public boolean notEnoughValues(Map vector) {
                    return false;
                }
            };

        }
        if (predicate instanceof False) {
            return new EsPredicate() {
                @Override
                public boolean match(Map<String, Object> vector) {
                    return false;
                }

                @Override
                public boolean notEnoughValues(Map vector) {
                    return false;
                }
            };
        }
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            List<EsPredicate> predicates = new ArrayList<>();
            for (Predicate childPredicate : ((CompoundPredicate) predicate).getPredicates()) {
                predicates.add(createPredicate(childPredicate, fieldTypeMap));
            }
            if (compoundPredicate.getBooleanOperator().value().equals("and")) {
                return new EsCompoundPredicate(predicates) {

                    @Override
                    protected boolean matchList(Map vector) {
                        boolean result = true;
                        for (EsPredicate childPredicate : predicates) {
                            result = result && childPredicate.match(vector);
                        }
                        return result;
                    }
                };
            }
            if (compoundPredicate.getBooleanOperator().value().equals("or")) {
                return new EsCompoundPredicate(predicates) {

                    @Override
                    protected boolean matchList(Map vector) {
                        boolean result = false;
                        for (EsPredicate childPredicate : predicates) {
                            result = result || childPredicate.match(vector);
                        }
                        return result;
                    }
                };
            }
            if (compoundPredicate.getBooleanOperator().value().equals("xor")) {
                return new EsCompoundPredicate(predicates) {

                    @Override
                    protected boolean matchList(Map vector) {
                        boolean result = false;
                        for (EsPredicate childPredicate : predicates) {
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
                return new EsCompoundPredicate(predicates) {

                    @Override
                    protected boolean matchList(Map vector) {
                        for (EsPredicate childPredicate : predicates) {
                            if (childPredicate.notEnoughValues(vector) == false) {
                                return childPredicate.match(vector);

                            }
                        }
                        return false;
                    }

                    @Override
                    public boolean notEnoughValues(Map vector) {
                        boolean notEnoughValues = true;
                        for (EsPredicate predicate : predicates) {
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
                return new EsSimpleSetPredicate(valuesSet, field);
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
                return new EsSimpleSetPredicate(valuesSet, field);
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
                return new EsSimpleSetPredicate(valuesSet, field);
            }
        }
        throw new UnsupportedOperationException("Predicate Type " + predicate.getClass().getName() + " for TreeModel not implemented yet.");
    }

    protected static <T extends Comparable> EsSimplePredicate<T> getSimplePredicate(T value, String field, String operator) {
        if (operator.equals("equal")) {
            return new EsSimplePredicate<T>(value, field) {
                @Override
                public boolean match(T fieldValue) {
                    return value.equals(fieldValue);
                }
            };
        }
        if (operator.equals("notEqual")) {
            return new EsSimplePredicate<T>(value, field) {
                @Override
                public boolean match(T fieldValue) {
                    return value.equals(fieldValue) == false;
                }
            };
        }
        if (operator.equals("lessThan")) {
            return new EsSimplePredicate<T>(value, field) {
                @Override
                public boolean match(T fieldValue) {
                    return fieldValue.compareTo(value) < 0;
                }
            };
        }
        if (operator.equals("lessOrEqual")) {
            return new EsSimplePredicate<T>(value, field) {
                @Override
                public boolean match(T fieldValue) {
                    return fieldValue.compareTo(value) <= 0;
                }
            };
        }
        if (operator.equals("greaterThan")) {
            return new EsSimplePredicate<T>(value, field) {
                @Override
                public boolean match(T fieldValue) {
                    return fieldValue.compareTo(value) > 0;
                }
            };
        }
        if (operator.equals("greaterOrEqual")) {
            return new EsSimplePredicate<T>(value, field) {
                @Override
                public boolean match(T fieldValue) {
                    return fieldValue.compareTo(value) >= 0;
                }
            };
        }
        if (operator.equals("isMissing")) {
            return new EsSimplePredicate<T>(value, field) {
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
            return new EsSimplePredicate<T>(value, field) {
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

    abstract static class EsPredicate {
        public EsPredicate() {

        }

        public abstract boolean match(Map<String, Object> vector);

        public abstract boolean notEnoughValues(Map vector);
    }

    abstract static class EsSimplePredicate<T extends Comparable> extends EsPredicate {

        protected final T value;
        protected String field;

        public EsSimplePredicate(T value, String field) {

            this.value = value;
            this.field = field;
        }

        public abstract boolean match(T fieldValue);

        public boolean match(Map<String, Object> vector) {
            Object fieldValue = vector.get(field);
            if (fieldValue instanceof HashSet) {
                fieldValue = new ComparableSet((HashSet) fieldValue);
            }
            if (fieldValue == null) {
                return false;
            }
            return match((T) fieldValue);
        }

        @Override
        public boolean notEnoughValues(Map vector) {
            return vector.containsKey(field) == false;
        }
    }

    abstract static class EsCompoundPredicate extends EsPredicate {

        protected List<EsPredicate> predicates;

        public EsCompoundPredicate(List<EsPredicate> predicates) {
            this.predicates = predicates;
        }

        public boolean match(Map<String, Object> vector) {
            return matchList(vector);
        }

        protected abstract boolean matchList(Map<String, Object> vector);

        @Override
        public boolean notEnoughValues(Map vector) {
            boolean valuesMissing = false;
            for (EsPredicate predicate : predicates) {
                valuesMissing = predicate.notEnoughValues(vector) || valuesMissing;
            }
            return valuesMissing;
        }
    }

    static class EsSimpleSetPredicate extends EsPredicate {

        protected HashSet values;
        private String field;

        public EsSimpleSetPredicate(HashSet values, String field) {
            this.values = values;
            this.field = field;
        }


        @Override
        public boolean match(Map<String, Object> vector) {
            // we do not check for null because HashSet allows null values.
            for (Object value : (Set)vector.get(field))  {
                if (values.contains(value)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean notEnoughValues(Map vector) {
            return vector.containsKey(field) == false;
        }
    }

    public static class ComparableSet extends HashSet<Comparable> implements Comparable {

        public ComparableSet(HashSet<Comparable> set) {
            this.addAll(set);
        }
        @Override
        public int compareTo(Object o) {
            if (this.size()!= 1) {
                throw new UnsupportedOperationException("cannot really compare sets, I am just pretending!");
            }
            if (o instanceof Comparable == false) {
                throw new UnsupportedOperationException("cannot compare to object " + o.getClass().getName());
            }
            return this.toArray(new Comparable[1])[0].compareTo((Comparable)o);
        }
    }
}
