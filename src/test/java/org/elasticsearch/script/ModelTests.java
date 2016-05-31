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

import org.apache.commons.io.FileUtils;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.linalg.DenseVector;
import org.dmg.pmml.*;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.script.pmml.PMMLModelScriptEngineService;
import org.elasticsearch.test.ESTestCase;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.junit.Test;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 */
public class ModelTests extends ESTestCase {


    @Test
    // only just checks that nothing crashes
    // compares to mllib and fails every now and then because we do not consider the margin
    public void testMLLibVsEsSVM() throws IOException, JAXBException, SAXException {
        for (int i = 0; i < 100; i++) {

            double[] modelParams = {randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100)};
            SVMModel svmm = new SVMModel(new DenseVector(modelParams), 0.1);
            String pmmlString = PMMLGenerator.generateSVMPMMLModel(0.1, modelParams, new double[]{1, 0});
            EsModelEvaluator esLinearSVMModel = PMMLModelScriptEngineService.Factory.initModelWithoutPreProcessing(pmmlString);
            assertThat(esLinearSVMModel, instanceOf(EsLinearSVMModel.class));
            Map<FieldName, Object> params = new HashMap<>();
            int[] vals = new int[]{1, 1, 1, 0};//{randomIntBetween(0, +100), randomIntBetween(0, +100), randomIntBetween(0, +100), 0};
            params.put(new FieldName("field_0"), new Double(vals[0]));
            params.put(new FieldName("field_1"), new Double(vals[1]));
            params.put(new FieldName("field_2"), new Double(vals[2]));
            String result = esLinearSVMModel.evaluate(new Tuple<>(new int[]{0, 1, 2}, new double[]{vals[0], vals[1], vals[2]}));
            double mllibResult = svmm.predict(new DenseVector(new double[]{vals[0], vals[1], vals[2]}));
            assertThat(mllibResult, equalTo(Double.parseDouble(result)));

            EsModelEvaluator esSVMModel = new EsLinearSVMModel(modelParams, 0.1, new String[]{"1", "0"});
            String esLabel = esSVMModel.evaluate(new Tuple<>(new int[]{0, 1, 2}, new double[]{vals[0], vals[1], vals[2]}));
            assertThat(mllibResult, equalTo(Double.parseDouble(esLabel)));
        }
    }

    @Test
    // only just checks that nothing crashes
    public void testMLLibVsEsLLR() throws IOException, JAXBException, SAXException {
        for (int i = 0; i < 10; i++) {

            double[] modelParams = new double[]{randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100)};
            LogisticRegressionModel lrm = new LogisticRegressionModel(new DenseVector(modelParams), 0.1);
            String pmmlString = PMMLGenerator.generateLRPMMLModel(0.1, modelParams, new double[]{1, 0});
            EsModelEvaluator esLogisticRegressionModel =  PMMLModelScriptEngineService.Factory.initModelWithoutPreProcessing(pmmlString);
            assertThat(esLogisticRegressionModel, instanceOf(EsLogisticRegressionModel.class));
            Map<FieldName, Object> params = new HashMap<>();
            int[] vals = new int[]{1, 1, 1, 0};//{randomIntBetween(0, +100), randomIntBetween(0, +100), randomIntBetween(0, +100), 0};
            params.put(new FieldName("field_0"), new Double(vals[0]));
            params.put(new FieldName("field_1"), new Double(vals[1]));
            params.put(new FieldName("field_2"), new Double(vals[2]));
            params.put(new FieldName("field_3"), new Double(vals[3]));
            double mllibResult = lrm.predict(new DenseVector(new double[]{vals[0], vals[1], vals[2], vals[3]}));
            String result = esLogisticRegressionModel.evaluate(new Tuple<>(new int[]{0, 1, 2}, new double[]{vals[0], vals[1], vals[2]}));
            assertThat(mllibResult, equalTo(Double.parseDouble(result)));

            EsModelEvaluator esLLRModel = new EsLogisticRegressionModel(modelParams, 0.1, new String[]{"1", "0"});
            String esLabel = esLLRModel.evaluate(new Tuple<>(new int[]{0, 1, 2}, new double[]{vals[0], vals[1], vals[2]}));
            assertThat(mllibResult, equalTo(Double.parseDouble(esLabel)));

        }
    }

    @Test
    // only just checks that nothing crashes
    public void testMLLibVsEsNB() throws IOException, JAXBException, SAXException {
        for (int i = 0; i < 1000; i++) {

            double[][] thetas = new double[][]{{randomFloat() * randomIntBetween(-100, -1), randomFloat() * randomIntBetween(-100, -1), randomFloat() * randomIntBetween(-100, -1), randomFloat() * randomIntBetween(-100, -1)},
                    {randomFloat() * randomIntBetween(-100, -1), randomFloat() * randomIntBetween(-100, -1), randomFloat() * randomIntBetween(-100, -1), randomFloat() * randomIntBetween(-100, -1)}};
            double[] pis = new double[]{randomFloat() * randomIntBetween(-100, -1), randomFloat() * randomIntBetween(-100, -1)};
            String[] labels = {"0", "1"};
            double[] labelsAsDoubles = {0.0d, 1.0d};
            NaiveBayesModel nb = new NaiveBayesModel(labelsAsDoubles, pis, thetas);
            EsModelEvaluator esNaiveBayesModel = new EsNaiveBayesModel(thetas, pis, labels);
            int[] vals = {randomIntBetween(0, +10), randomIntBetween(0, +10), randomIntBetween(0, +10), randomIntBetween(0, +10)};
            double mllibResult = nb.predict(new DenseVector(new double[]{vals[0], vals[1], vals[2], vals[3]}));
            String result = esNaiveBayesModel.evaluate(new Tuple<>(new int[]{0, 1, 2, 3}, new double[]{vals[0], vals[1], vals[2], vals[3]}));
            assertThat(mllibResult, equalTo(Double.parseDouble(result)));
        }
    }


    @Test
    @AwaitsFix(bugUrl = "needs to be replaced or removed")
    public void testTextIndex() throws IOException, JAXBException, SAXException {
        try (InputStream is = new ByteArrayInputStream(FileUtils.readFileToByteArray(FileUtils.getFile("/Users/britta/Downloads/test.xml")))) {
            URL schemaFile = new URL("http://dmg.org/pmml/v4-2-1/pmml-4-2.xsd");
            Source xmlFile = new StreamSource(is);
            SchemaFactory schemaFactory = SchemaFactory
                    .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = schemaFactory.newSchema(schemaFile);
            Validator validator = schema.newValidator();
            try {
                validator.validate(xmlFile);
                System.out.println(xmlFile.getSystemId() + " is valid");
            } catch (SAXException e) {
                System.out.println(xmlFile.getSystemId() + " is NOT valid");
                System.out.println("Reason: " + e.getMessage());
            }
        }
    }

    @Test
    @AwaitsFix(bugUrl = "needs to be replaced or removed")
    public void checkMLlibPMMLOutputValid() throws IOException, JAXBException, SAXException {
        try (InputStream is = new ByteArrayInputStream(FileUtils.readFileToByteArray(FileUtils.getFile("/Users/britta/tmp/test.xml")))) {
            URL schemaFile = new URL("http://dmg.org/pmml/v4-2-1/pmml-4-2.xsd");
            Source xmlFile = new StreamSource(is);
            SchemaFactory schemaFactory = SchemaFactory
                    .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = schemaFactory.newSchema(schemaFile);
            Validator validator = schema.newValidator();
            try {
                validator.validate(xmlFile);
                System.out.println(xmlFile.getSystemId() + " is valid");
            } catch (SAXException e) {
                System.out.println(xmlFile.getSystemId() + " is NOT valid");
                System.out.println("Reason: " + e.getMessage());
            }
        }
    }

    public void testGenerateLRPMML() throws JAXBException, IOException, SAXException {

        double[] weights = new double[]{randomDouble(), randomDouble(), randomDouble(), randomDouble()};
        double intercept = randomDouble();


        String generatedPMMLModel = PMMLGenerator.generateLRPMMLModel(intercept, weights, new double[]{1, 0});
        PMML hopefullyCorrectPMML;
        try (InputStream is = new ByteArrayInputStream(generatedPMMLModel.getBytes(Charset.defaultCharset()))) {
            Source transformedSource = ImportFilter.apply(new InputSource(is));
            hopefullyCorrectPMML = JAXBUtil.unmarshalPMML(transformedSource);
        }

        String pmmlString = "<PMML xmlns=\"http://www.dmg.org/PMML-4_2\">\n" +
                "    <DataDictionary numberOfFields=\"5\">\n" +
                "        <DataField name=\"field_0\" optype=\"continuous\" dataType=\"double\"/>\n" +
                "        <DataField name=\"field_1\" optype=\"continuous\" dataType=\"double\"/>\n" +
                "        <DataField name=\"field_2\" optype=\"continuous\" dataType=\"double\"/>\n" +
                "        <DataField name=\"field_3\" optype=\"continuous\" dataType=\"double\"/>\n" +
                "        <DataField name=\"target\" optype=\"categorical\" dataType=\"string\"/>\n" +
                "    </DataDictionary>\n" +
                "    <RegressionModel modelName=\"logistic regression\" functionName=\"classification\" normalizationMethod=\"logit\">\n" +
                "        <MiningSchema>\n" +
                "            <MiningField name=\"field_0\" usageType=\"active\"/>\n" +
                "            <MiningField name=\"field_1\" usageType=\"active\"/>\n" +
                "            <MiningField name=\"field_2\" usageType=\"active\"/>\n" +
                "            <MiningField name=\"field_3\" usageType=\"active\"/>\n" +
                "            <MiningField name=\"target\" usageType=\"target\"/>\n" +
                "        </MiningSchema>\n" +
                "        <RegressionTable intercept=\"" + Double.toString(intercept) + "\" targetCategory=\"1\">\n" +
                "            <NumericPredictor name=\"field_0\" coefficient=\"" + weights[0] + "\"/>\n" +
                "            <NumericPredictor name=\"field_1\" coefficient=\"" + weights[1] + "\"/>\n" +
                "            <NumericPredictor name=\"field_2\" coefficient=\"" + weights[2] + "\"/>\n" +
                "            <NumericPredictor name=\"field_3\" coefficient=\"" + weights[3] + "\"/>\n" +
                "        </RegressionTable>\n" +
                "        <RegressionTable intercept=\"-0.0\" targetCategory=\"0\"/>\n" +
                "    </RegressionModel>\n" +
                "</PMML>";
        PMML truePMML;
        try (InputStream is = new ByteArrayInputStream(pmmlString.getBytes(Charset.defaultCharset()))) {
            Source transformedSource = ImportFilter.apply(new InputSource(is));
            truePMML = JAXBUtil.unmarshalPMML(transformedSource);
        }

        compareModels(truePMML, hopefullyCorrectPMML);
    }


    public void testGenerateSVMPMML() throws JAXBException, IOException, SAXException {

        double[] weights = new double[]{randomDouble(), randomDouble(), randomDouble(), randomDouble()};
        double intercept = randomDouble();


        String generatedPMMLModel = PMMLGenerator.generateSVMPMMLModel(intercept, weights, new double[]{1, 0});
        PMML hopefullyCorrectPMML;
        try (InputStream is = new ByteArrayInputStream(generatedPMMLModel.getBytes(Charset.defaultCharset()))) {
            Source transformedSource = ImportFilter.apply(new InputSource(is));
            hopefullyCorrectPMML = JAXBUtil.unmarshalPMML(transformedSource);
        }

        String pmmlString = "<PMML xmlns=\"http://www.dmg.org/PMML-4_2\">\n" +
                "    <DataDictionary numberOfFields=\"5\">\n" +
                "        <DataField name=\"field_0\" optype=\"continuous\" dataType=\"double\"/>\n" +
                "        <DataField name=\"field_1\" optype=\"continuous\" dataType=\"double\"/>\n" +
                "        <DataField name=\"field_2\" optype=\"continuous\" dataType=\"double\"/>\n" +
                "        <DataField name=\"field_3\" optype=\"continuous\" dataType=\"double\"/>\n" +
                "        <DataField name=\"target\" optype=\"categorical\" dataType=\"string\"/>\n" +
                "    </DataDictionary>\n" +
                "    <RegressionModel modelName=\"linear SVM\" functionName=\"classification\" normalizationMethod=\"none\">\n" +
                "        <MiningSchema>\n" +
                "            <MiningField name=\"field_0\" usageType=\"active\"/>\n" +
                "            <MiningField name=\"field_1\" usageType=\"active\"/>\n" +
                "            <MiningField name=\"field_2\" usageType=\"active\"/>\n" +
                "            <MiningField name=\"field_3\" usageType=\"active\"/>\n" +
                "            <MiningField name=\"target\" usageType=\"target\"/>\n" +
                "        </MiningSchema>\n" +
                "        <RegressionTable intercept=\"" + intercept + "\" targetCategory=\"1\">\n" +
                "            <NumericPredictor name=\"field_0\" coefficient=\"" + weights[0] + "\"/>\n" +
                "            <NumericPredictor name=\"field_1\" coefficient=\"" + weights[1] + "\"/>\n" +
                "            <NumericPredictor name=\"field_2\" coefficient=\"" + weights[2] + "\"/>\n" +
                "            <NumericPredictor name=\"field_3\" coefficient=\"" + weights[3] + "\"/>\n" +
                "        </RegressionTable>\n" +
                "        <RegressionTable intercept=\"0.0\" targetCategory=\"0\"/>\n" +
                "    </RegressionModel>\n" +
                "</PMML>";
        PMML truePMML;
        try (InputStream is = new ByteArrayInputStream(pmmlString.getBytes(Charset.defaultCharset()))) {
            Source transformedSource = ImportFilter.apply(new InputSource(is));
            truePMML = JAXBUtil.unmarshalPMML(transformedSource);
        }
        compareModels(truePMML, hopefullyCorrectPMML);
    }

    public void compareModels(PMML model1, PMML model2) {
        assertThat(model1.getDataDictionary().getNumberOfFields(), equalTo(model2.getDataDictionary().getNumberOfFields()));
        int i = 0;
        for (DataField dataField : model1.getDataDictionary().getDataFields()) {
            DataField otherDataField = model2.getDataDictionary().getDataFields().get(i);
            assertThat(dataField.getDataType(), equalTo(otherDataField.getDataType()));
            assertThat(dataField.getName(), equalTo(otherDataField.getName()));
            i++;
        }

        assertThat(model1.getModels().size(), equalTo(model2.getModels().size()));
        i = 0;
        for (Model model : model1.getModels()) {
            if (model.getModelName().equals("linear SVM")) {
                assertThat(model, instanceOf(RegressionModel.class));
                assertThat(model2.getModels().get(i), instanceOf(RegressionModel.class));
                compareModels((RegressionModel) model, (RegressionModel) model2.getModels().get(i));
            } else if (model.getModelName().equals("logistic regression")) {
                assertThat(model, instanceOf(RegressionModel.class));
                assertThat(model2.getModels().get(i), instanceOf(RegressionModel.class));
                compareModels((RegressionModel) model, (RegressionModel) model2.getModels().get(i));
            } else {
                throw new UnsupportedOperationException("model " + model.getAlgorithmName() + " is not supported and therfore not tested yet");
            }
            i++;
        }
    }

    private static void compareModels(RegressionModel model1, RegressionModel model2) {
        assertThat(model1.getFunctionName().value(), equalTo(model2.getFunctionName().value()));
        assertThat(model1.getFunctionName().value(), equalTo(model2.getFunctionName().value()));
        assertThat(model1.getNormalizationMethod().value(), equalTo(model2.getNormalizationMethod().value()));
        compareMiningFields(model1, model2);
    }

    private static void compareMiningFields(Model model1, Model model2) {
        int i = 0;
        for (MiningField miningField : model1.getMiningSchema().getMiningFields()) {
            MiningField otherMiningField = model2.getMiningSchema().getMiningFields().get(i);
            compareMiningFields(miningField, otherMiningField);
            i++;
        }
    }

    private static void compareMiningFields(MiningField miningField, MiningField otherMiningField) {
        assertThat(miningField.getName(), equalTo(otherMiningField.getName()));
        assertThat(miningField.getUsageType().value(), equalTo(otherMiningField.getUsageType().value()));
    }

}
