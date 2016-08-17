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

import org.elasticsearch.script.modelinput.MapModelInput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EsTreeModel extends EsModelEvaluator<MapModelInput, String> {

    private final EsTreeNode startNode;

    public EsTreeModel(EsTreeNode startNode) {
        this.startNode = startNode;
    }

    @Override
    public Map<String, Object> evaluateDebug(MapModelInput modelInput) {
        Map<String, Object> vector = modelInput.getAsMap();
        assert startNode.predicate.match(vector);
        return startNode.evaluate(vector);
    }

    @Override
    public String evaluate(MapModelInput modelInput) {
        Map<String, Object> vector = modelInput.getAsMap();
        assert startNode.predicate.match(vector);
        return (String)startNode.evaluate(vector).get("class");
    }

    public static class EsTreeNode {
        EsPredicate predicate;
        java.util.List<EsTreeNode> childNodes = new ArrayList<>();
        String score;

        public EsTreeNode(List<EsTreeNode> childNodes, EsPredicate predicate, String score) {
            this.predicate = predicate;
            this.childNodes = childNodes;
            this.score = score;
        }


        private Map<String, Object> evaluate(Map<String, Object> vector) {
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

    public interface EsPredicate {
        boolean match(Map<String, Object> vector);

        boolean notEnoughValues(Map<String, Object> vector);
    }

    public abstract static class EsSimplePredicate<T extends Comparable<T>> implements EsPredicate {

        protected final T value;
        protected String field;

        protected EsSimplePredicate(T value, String field) {

            this.value = value;
            this.field = field;
        }

        public abstract boolean match(T fieldValue);

        @SuppressWarnings("unchecked")
        public boolean match(Map<String, Object> vector) {
            Object fieldValue = vector.get(field);
            if (fieldValue instanceof HashSet) {
                fieldValue = new ComparableSet<>((HashSet<Comparable<T>>) fieldValue);
            }
            if (fieldValue == null) {
                return false;
            }
            return match((T) fieldValue);
        }

        @Override
        public boolean notEnoughValues(Map<String, Object> vector) {
            return vector.containsKey(field) == false;
        }
    }

    public abstract static class EsCompoundPredicate implements EsPredicate {

        protected List<EsPredicate> predicates;

        protected EsCompoundPredicate(List<EsPredicate> predicates) {
            this.predicates = predicates;
        }

        public boolean match(Map<String, Object> vector) {
            return matchList(vector);
        }

        protected abstract boolean matchList(Map<String, Object> vector);

        @Override
        public boolean notEnoughValues(Map<String, Object> vector) {
            boolean valuesMissing = false;
            for (EsPredicate predicate : predicates) {
                valuesMissing = predicate.notEnoughValues(vector) || valuesMissing;
            }
            return valuesMissing;
        }
    }

    public static class EsSimpleSetPredicate<T> implements EsPredicate {

        protected HashSet<T> values;
        private String field;

        public EsSimpleSetPredicate(HashSet<T> values, String field) {
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
        public boolean notEnoughValues(Map<String, Object> vector) {
            return vector.containsKey(field) == false;
        }
    }

    public static class ComparableSet<T> extends HashSet<Comparable<T>> implements Comparable<T> {

        public ComparableSet(HashSet<Comparable<T>> set) {
            this.addAll(set);
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compareTo(T o) {
            if (this.size()!= 1) {
                throw new UnsupportedOperationException("cannot really compare sets, I am just pretending!");
            }
            if (o instanceof Comparable == false) {
                throw new UnsupportedOperationException("cannot compare to object " + o.getClass().getName());
            }
            //noinspection unchecked
            Comparable<T> first = this.iterator().next();
            return first.compareTo(o);
        }
    }
}
