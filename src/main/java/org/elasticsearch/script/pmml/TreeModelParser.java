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

package org.elasticsearch.script.pmml;


import org.dmg.pmml.CompoundPredicate;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.Node;
import org.dmg.pmml.PMML;
import org.dmg.pmml.Predicate;
import org.dmg.pmml.SimplePredicate;
import org.dmg.pmml.SimpleSetPredicate;
import org.dmg.pmml.TransformationDictionary;
import org.dmg.pmml.TreeModel;
import org.elasticsearch.script.modelinput.VectorRange;
import org.elasticsearch.script.modelinput.VectorRangesToVectorPMML;
import org.elasticsearch.script.modelinput.PMMLVectorRange;
import org.elasticsearch.script.models.EsTreeModel;
import org.elasticsearch.script.models.MapModelInput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TreeModelParser extends ModelParser<MapModelInput, TreeModel> {

    public TreeModelParser() {
        super(TreeModel.class);
    }

    @Override
    public ModelAndInputEvaluator<MapModelInput> parse(TreeModel treeModel, DataDictionary dataDictionary,
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
            return new ModelAndInputEvaluator<>(fieldsToVector, esTreeModel);
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

    protected static EsTreeModel getEsTreeModel(TreeModel treeModel, Map<String, String> fieldToTypeMap) {
        return new EsTreeModel(treeModel, fieldToTypeMap);
    }

    public static Map<String,String> getFieldToTypeMap(java.util.List<VectorRange> vectorRangeList) {
        Map<String, String> fieldToTypeMap = new HashMap<>();
        for (VectorRange vectorRange : vectorRangeList) {
            fieldToTypeMap.put(vectorRange.getLastDerivedFieldName(), vectorRange.getType());
        }
        return fieldToTypeMap;
    }
}
