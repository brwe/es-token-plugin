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

import java.util.HashMap;
import java.util.Map;

public class EsTreeModel extends EsModelEvaluator {

    private TreeModel treeModel;
    private Map<String, String> fieldTypeMap;

    public EsTreeModel(TreeModel treeModel, Map<String, String> fieldTypeMap) {

        this.treeModel = treeModel;
        this.fieldTypeMap = fieldTypeMap;
    }

    @Override
    public Map<String, Object> evaluate(Map<String, Object> vector) {
        Node startNode = treeModel.getNode();
        assert checkPredicate(startNode.getPredicate(), vector) == true;
        return evaluate(startNode, vector);
    }

    private Map<String, Object> evaluate(Node node, Map<String, Object> vector) {
        for (Node childNode : node.getNodes()) {
            if (checkPredicate(childNode.getPredicate(), vector)) {
                return evaluate(childNode, vector);
            }
        }
        HashMap<String, Object> result = new HashMap<>();
        result.put("class", node.getScore());
        return result;
    }

    private boolean checkPredicate(Predicate predicate, Map<String, Object> vector) {
        if(predicate instanceof True) {
            return true;
        }
        if(predicate instanceof False) {
            return false;
        }
        if (predicate instanceof SimplePredicate) {
            SimplePredicate simplePredicate = (SimplePredicate)predicate;
            //simplePredicate.getField().
        }
        return false;
    }

}
