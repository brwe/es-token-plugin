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

import org.dmg.pmml.False;
import org.dmg.pmml.Node;
import org.dmg.pmml.Predicate;
import org.dmg.pmml.SimplePredicate;
import org.dmg.pmml.TreeModel;
import org.dmg.pmml.True;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class EsTreeModel extends EsModelEvaluator {

    private TreeModel treeModel;
    EsTreeNode startNode;

    public EsTreeModel(TreeModel treeModel, Map<String, String> fieldTypeMap) {

        startNode = new EsTreeNode(treeModel.getNode(), fieldTypeMap);
        this.treeModel = treeModel;
    }

    @Override
    public Map<String, Object> evaluate(Map<String, Object> vector) {
        assert startNode.predicate.match(vector);
        return startNode.evaluate(vector);
    }

    class EsTreeNode {
        EsPredicate predicate;
        java.util.List<EsTreeNode> childNodes = new ArrayList<>();
        String score;

        public EsTreeNode(Node node, Map<String, String> fieldTypeMap) {
            Node startNode = treeModel.getNode();
            predicate = createPredicate(startNode.getPredicate(), fieldTypeMap);
            for (Node childNode : startNode.getNodes()) {
                childNodes.add(new EsTreeNode(childNode, fieldTypeMap));
            }
            score = node.getScore();
        }

        public Map<String, Object> evaluate(Map<String, Object> vector) {
            for(EsTreeNode childNode : childNodes) {
                if (childNode.predicate.match(vector)) {
                    return childNode.evaluate(vector);
                }
            }
            Map<String, Object> result = new HashMap<>();
            result.put("class", score);
            return result;
        }
    }

    private EsPredicate createPredicate(Predicate predicate, Map<String, String> fieldTypeMap) {
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
            };
        }
        if (predicate instanceof False) {
            return new EsPredicate() {
                @Override
                public boolean match(Map<String, Object> vector) {
                    return false;
                }
            };
        }
        throw new UnsupportedOperationException("Predicate Type " + predicate.getClass().getName() + " for TreeModel not implemented yet.");
    }

    private  <T extends Comparable> EsSimplePredicate<T> getSimplePredicate(T value, String field, String operator) {
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
    }

    abstract class EsPredicate {

        public EsPredicate() {
        }

        public EsPredicate(Predicate predicate, Map<String, String> fieldTypeMap) {

        }

        public abstract boolean match(Map<String, Object> vector);
    }

    abstract class EsSimplePredicate<T extends Comparable> extends EsPredicate {

        protected final T value;
        protected String field;

        public EsSimplePredicate(T value, String field) {

            this.value = value;
            this.field = field;
        }

        public abstract boolean match(T fieldValue);

        public boolean match(Map<String, Object> vector) {
            Object fieldValue = vector.get(field);
            if (fieldValue == null) {
                return false;
            }
            return match((T) fieldValue);
        }
    }
}
