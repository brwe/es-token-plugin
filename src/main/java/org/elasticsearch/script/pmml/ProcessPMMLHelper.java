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


import org.dmg.pmml.Apply;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.Expression;
import org.dmg.pmml.FieldRef;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.Model;
import org.dmg.pmml.NormContinuous;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.TransformationDictionary;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.ml.modelinput.PMMLVectorRange;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.transform.Source;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ProcessPMMLHelper {

    public static DataField getRawDataField(DataDictionary dataDictionary, String rawFieldName) {
        // now find the actual dataField
        DataField rawField = null;
        for (DataField dataField : dataDictionary.getDataFields()) {
            String rawDataFieldName = dataField.getName().getValue();
            if (rawDataFieldName.equals(rawFieldName)) {
                rawField = dataField;
                break;
            }
        }
        if (rawField == null) {
            throw new UnsupportedOperationException("Could not trace back {} to a raw input field. Maybe saomething is not implemented " +
                    "yet or the PMML file is faulty.");
        }
        return rawField;
    }

    // allDerivedFields is a list of all derived fields that are to be considered
    // derivedFields is the list of derived fields for fieldName
    // returns the last derived field that was found
    public static String getDerivedFields(String fieldName, List<DerivedField> allDerivedFields, List<DerivedField>
            derivedFields) {
        // trace back all derived fields until we must arrive at an actual data field. This unfortunately means we have to
        // loop over dervived fields as often as we find one..
        DerivedField lastFoundDerivedField;
        String lastFieldName = fieldName;

        do {
            lastFoundDerivedField = null;
            for (DerivedField derivedField : allDerivedFields) {
                if (derivedField.getName().getValue().equals(lastFieldName)) {
                    lastFoundDerivedField = derivedField;
                    derivedFields.add(derivedField);
                    // now get the next fieldname this field references
                    // this is tricky, because this information can be anywhere...
                    lastFieldName = getReferencedFieldName(derivedField);
                    lastFoundDerivedField = derivedField;
                }
            }
        } while (lastFoundDerivedField != null);
        return lastFieldName;
    }

    private static String getReferencedFieldName(DerivedField derivedField) {
        String referencedField = null;
        if (derivedField.getExpression() != null) {
            if (derivedField.getExpression() instanceof Apply) {
                // TODO throw uoe in case the function is not "if missing" - much more to implement!
                for (Expression expression : ((Apply) derivedField.getExpression()).getExpressions()) {
                    if (expression instanceof FieldRef) {
                        referencedField = ((FieldRef) expression).getField().getValue();
                    }
                }
            } else if (derivedField.getExpression() instanceof NormContinuous) {
                referencedField = ((NormContinuous) derivedField.getExpression()).getField().getValue();
            } else {
                throw new UnsupportedOperationException("So far only Apply expression implemented.");
            }
        } else {
            // there is a million ways in which derived fields can reference other fields.
            // need to implement them all!
            throw new UnsupportedOperationException("So far only implemented if function for derived fields.");
        }

        if (referencedField == null) {
            throw new UnsupportedOperationException("could not find raw field name. Maybe this derived field references another derived " +
                    "field? Did not implement that yet.");
        }
        return referencedField;
    }

    public static PMML parsePmml(final String pmmlString) {
        // this is bad but I have not figured out yet how to avoid the permission for suppressAccessCheck
        return AccessController.doPrivileged(new PrivilegedAction<PMML>() {
            public PMML run() {
                try (InputStream is = new ByteArrayInputStream(pmmlString.getBytes(Charset.defaultCharset()))) {
                    Source transformedSource = ImportFilter.apply(new InputSource(is));
                    return JAXBUtil.unmarshalPMML(transformedSource);
                } catch (SAXException e) {
                    throw new ElasticsearchException("could not convert xml to pmml model", e);
                } catch (JAXBException e) {
                    throw new ElasticsearchException("could not convert xml to pmml model", e);
                } catch (IOException e) {
                    throw new ElasticsearchException("could not convert xml to pmml model", e);
                }
            }
        });
    }

    public static List<DerivedField> getAllDerivedFields(Model model, TransformationDictionary transformationDictionary) {
        List<DerivedField> allDerivedFields = new ArrayList<>();
        if (transformationDictionary != null) {
            allDerivedFields.addAll(transformationDictionary.getDerivedFields());
        }
        if (model.getLocalTransformations() != null) {
            allDerivedFields.addAll(model.getLocalTransformations().getDerivedFields());
        }
        return allDerivedFields;
    }

    public static MiningField getMiningField(Model model, String rawFieldName) {
        MiningField miningField = null;
        // also pass in the mining schema for additional parameters
        for (MiningField aMiningField : model.getMiningSchema().getMiningFields()) {
            if (aMiningField.getKey().getValue().equals(rawFieldName)) {
                miningField = aMiningField;
            }
        }
        return miningField;
    }

    public static PMMLVectorRange extractVectorRange(Model model, DataDictionary dataDictionary,
                                               TransformationDictionary transformationDictionary,
                                               String fieldName, Provider<Collection<String>> categories,
                                               int position, Map<String, OpType> types) {
        List<DerivedField> allDerivedFields = ProcessPMMLHelper.getAllDerivedFields(model, transformationDictionary);
        List<DerivedField> derivedFields = new ArrayList<>();
        String rawFieldName = ProcessPMMLHelper.getDerivedFields(fieldName, allDerivedFields, derivedFields);
        DataField rawField = ProcessPMMLHelper.getRawDataField(dataDictionary, rawFieldName);
        MiningField miningField = ProcessPMMLHelper.getMiningField(model, rawFieldName);
        PMMLVectorRange featureEntries = getFieldVector(position, derivedFields, rawField, miningField, categories, types);
        return featureEntries;
    }



    public static PMMLVectorRange getFieldVector(int indexCounter, List<DerivedField> derivedFields,
                                           DataField rawField, MiningField miningField,
                                           Provider<Collection<String>> categories,
                                           Map<String, OpType> types) {
        PMMLVectorRange featureEntries;
        OpType opType;
        if (derivedFields.size() == 0) {
            opType = rawField.getOpType();
        } else {
            opType = derivedFields.get(0).getOpType();
        }

        if (opType.equals(OpType.CONTINUOUS)) {
            featureEntries = new PMMLVectorRange.ContinousSingleEntryVectorRange(rawField, miningField,
                    derivedFields.toArray(new DerivedField[derivedFields.size()]));
            featureEntries.addVectorEntry(indexCounter, "dummyValue");
        } else if (opType.equals(OpType.CATEGORICAL)) {
            featureEntries = new PMMLVectorRange.SparseCategoricalVectorRange(rawField, miningField,
                    derivedFields.toArray(new DerivedField[derivedFields.size()]));
            for (String value : categories.get()) {
                featureEntries.addVectorEntry(indexCounter, value);
                indexCounter++;
            }
        } else {
            throw new UnsupportedOperationException("Only implemented continuous and categorical variables so far.");
        }
        if(types != null) {
            types.put(featureEntries.getLastDerivedFieldName(), opType);
        }
        return featureEntries;
    }
}
