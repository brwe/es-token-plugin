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

package org.elasticsearch.script;

import org.dmg.pmml.*;
import org.jpmml.model.JAXBUtil;

import javax.xml.bind.JAXBException;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

public class PMMLGenerator {
    public static String generateSVMPMMLModel(double intercept, double[] weights, double[] labels) throws JAXBException, UnsupportedEncodingException {
        PMML pmml = new PMML();
        // create DataDictionary
        DataDictionary dataDictionary = createDataDictionary(weights);
        pmml.setDataDictionary(dataDictionary);

        // create model
        RegressionModel regressionModel = new RegressionModel();
        regressionModel.setModelName("linear SVM");
        regressionModel.setFunctionName(MiningFunctionType.CLASSIFICATION);
        regressionModel.setNormalizationMethod(RegressionNormalizationMethodType.NONE);
        MiningSchema miningSchema = createMiningSchema(weights);
        regressionModel.setMiningSchema(miningSchema);
        RegressionTable regressionTable0 = new RegressionTable();
        regressionTable0.setIntercept(intercept);
        regressionTable0.setTargetCategory(Double.toString(labels[0]));
        NumericPredictor[] numericPredictors = createNumericPredictors(weights);
        regressionTable0.addNumericPredictors(numericPredictors);
        RegressionTable regressionTable1 = new RegressionTable();
        regressionTable1.setIntercept(0.0);
        regressionTable1.setTargetCategory(Double.toString(labels[1]));
        regressionModel.addRegressionTables(regressionTable0, regressionTable1);
        pmml.addModels(regressionModel);
        // marshal
        return convertPMMLToString(pmml);
        // write to string
    }

    public static String generateLRPMMLModel(double intercept, double[] weights, double[] labels) throws JAXBException, UnsupportedEncodingException {
        PMML pmml = new PMML();
        // create DataDictionary
        DataDictionary dataDictionary = createDataDictionary(weights);
        pmml.setDataDictionary(dataDictionary);

        // create model
        RegressionModel regressionModel = new RegressionModel();
        regressionModel.setModelName("logistic regression");
        regressionModel.setFunctionName(MiningFunctionType.CLASSIFICATION);
        regressionModel.setNormalizationMethod(RegressionNormalizationMethodType.LOGIT);
        MiningSchema miningSchema = createMiningSchema(weights);
        regressionModel.setMiningSchema(miningSchema);
        RegressionTable regressionTable0 = new RegressionTable();
        regressionTable0.setIntercept(intercept);
        regressionTable0.setTargetCategory(Double.toString(labels[0]));
        NumericPredictor[] numericPredictors = createNumericPredictors(weights);
        regressionTable0.addNumericPredictors(numericPredictors);
        RegressionTable regressionTable1 = new RegressionTable();
        regressionTable1.setIntercept(0.0);
        regressionTable1.setTargetCategory(Double.toString(labels[1]));
        regressionModel.addRegressionTables(regressionTable0, regressionTable1);
        pmml.addModels(regressionModel);
        // marshal
        return convertPMMLToString(pmml);
        // write to string
    }

    public static String convertPMMLToString(PMML pmml) throws JAXBException, UnsupportedEncodingException {
        ByteArrayOutputStream baor = new ByteArrayOutputStream();
        StreamResult streamResult = new StreamResult();
        streamResult.setOutputStream(baor);
        JAXBUtil.marshal(pmml, streamResult);
        return baor.toString(Charset.defaultCharset().toString());
    }

    public static DataDictionary createDataDictionary(double[] weights) {
        DataDictionary dataDictionary = new DataDictionary();
        DataField[] dataFields = new DataField[weights.length + 1];
        for (int i = 0; i < weights.length; i++) {
            dataFields[i] = createDataField("field_" + Integer.toString(i), DataType.DOUBLE, OpType.CONTINUOUS);
        }
        dataFields[weights.length] = createDataField("target", DataType.STRING, OpType.CATEGORICAL);
        dataDictionary.addDataFields(dataFields);
        dataDictionary.setNumberOfFields(weights.length + 1);
        return dataDictionary;
    }

    public static NumericPredictor[] createNumericPredictors(double[] weights) {
        NumericPredictor[] numericPredictors = new NumericPredictor[weights.length];
        for (int i = 0; i < weights.length; i++) {
            numericPredictors[i] = creatNumericPredictor("field_" + Integer.toString(i), weights[i]);
        }
        return numericPredictors;
    }

    public static MiningSchema createMiningSchema(double[] weights) {
        MiningSchema miningSchema = new MiningSchema();
        MiningField[] miningFields = new MiningField[weights.length + 1];
        for (int i = 0; i < weights.length; i++) {
            miningFields[i] = creatMiningField("field_" + Integer.toString(i), FieldUsageType.ACTIVE);
        }
        miningFields[weights.length] = creatMiningField("target", FieldUsageType.TARGET);
        miningSchema.addMiningFields(miningFields);
        return miningSchema;
    }

    private static NumericPredictor creatNumericPredictor(String name, double coefficient) {
        NumericPredictor numericPredictor = new NumericPredictor();
        numericPredictor.setCoefficient(coefficient);
        numericPredictor.setName(FieldName.create(name));
        return numericPredictor;
    }

    private static MiningField creatMiningField(String name, FieldUsageType fieldUsageType) {
        MiningField field = new MiningField();
        field.setName(new FieldName(name));
        field.setUsageType(fieldUsageType);

        return field;

    }

    public static DataField createDataField(String name, DataType datatype, OpType opType) {
        DataField field = new DataField();
        field.setName(new FieldName(name));
        field.setDataType(datatype);
        field.setOpType(opType);
        return field;
    }
}
