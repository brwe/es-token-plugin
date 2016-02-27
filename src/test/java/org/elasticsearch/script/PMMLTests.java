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
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.linalg.DenseVector;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.dmg.pmml.RegressionModel;
import org.elasticsearch.common.collect.Tuple;
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

/**
 */
public class PMMLTests extends ESTestCase {


    @Test
    // only just checks that nothing crashes
    // compares to mllib and fails every now and then because we do not consider the margin
    public void testPMMLSVM() throws IOException, JAXBException, SAXException {
        for (int i = 0; i < 100; i++) {

            double[] modelParams = {randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100)};
            SVMModel svmm = new SVMModel(new DenseVector(modelParams), 0.1);
            String pmmlString = "<PMML xmlns=\"http://www.dmg.org/PMML-4_2\">\n" +
                    "    <Header description=\"linear SVM\">\n" +
                    "        <Application name=\"Apache Spark MLlib\" version=\"1.5.2\"/>\n" +
                    "        <Timestamp>2015-12-30T13:51:42</Timestamp>\n" +
                    "    </Header>\n" +
                    "    <DataDictionary numberOfFields=\"4\">\n" +
                    "        <DataField name=\"field_0\" optype=\"continuous\" dataType=\"double\"/>\n" +
                    "        <DataField name=\"field_1\" optype=\"continuous\" dataType=\"double\"/>\n" +
                    "        <DataField name=\"field_2\" optype=\"continuous\" dataType=\"double\"/>\n" +
                    "        <DataField name=\"target\" optype=\"categorical\" dataType=\"string\"/>\n" +
                    "    </DataDictionary>\n" +
                    "    <RegressionModel modelName=\"linear SVM\" functionName=\"classification\" normalizationMethod=\"none\">\n" +
                    "        <MiningSchema>\n" +
                    "            <MiningField name=\"field_0\" usageType=\"active\"/>\n" +
                    "            <MiningField name=\"field_1\" usageType=\"active\"/>\n" +
                    "            <MiningField name=\"field_2\" usageType=\"active\"/>\n" +
                    "            <MiningField name=\"target\" usageType=\"target\"/>\n" +
                    "        </MiningSchema>\n" +
                    "        <RegressionTable intercept=\"0.1\" targetCategory=\"1\">\n" +
                    "            <NumericPredictor name=\"field_0\" coefficient=\"" + modelParams[0] + "\"/>\n" +
                    "            <NumericPredictor name=\"field_1\" coefficient=\"" + modelParams[1] + "\"/>\n" +
                    "            <NumericPredictor name=\"field_2\" coefficient=\"" + modelParams[2] + "\"/>\n" +
                    "        </RegressionTable>\n" +
                    "        <RegressionTable intercept=\"0.0\" targetCategory=\"0\"/>\n" +
                    "    </RegressionModel>\n" +
                    "</PMML>";

            PMML pmml;
            try (InputStream is = new ByteArrayInputStream(pmmlString.getBytes(Charset.defaultCharset()))) {
                Source transformedSource = ImportFilter.apply(new InputSource(is));
                pmml = JAXBUtil.unmarshalPMML(transformedSource);
            }
            EsLinearSVMModel esLinearSVMModel = new EsLinearSVMModel((RegressionModel) pmml.getModels().get(0));
            Map<FieldName, Object> params = new HashMap<>();
            int[] vals = new int[]{1, 1, 1, 0};//{randomIntBetween(0, +100), randomIntBetween(0, +100), randomIntBetween(0, +100), 0};
            params.put(new FieldName("field_0"), new Double(vals[0]));
            params.put(new FieldName("field_1"), new Double(vals[1]));
            params.put(new FieldName("field_2"), new Double(vals[2]));
            String result = esLinearSVMModel.evaluate(new Tuple<>(new int[]{0, 1, 2}, new double[]{vals[0], vals[1], vals[2]}));
            double mllibResult = svmm.predict(new DenseVector(new double[]{vals[0], vals[1], vals[2]}));
            assertThat(mllibResult, equalTo(Double.parseDouble(result)));
            // now try the same with pmml script

        }
    }

    @Test
    // only just checks that nothing crashes
    // compares to mllib and fails every now and then because we do not consider the margin
    public void testTextIndex() throws IOException, JAXBException, SAXException {

        double[] modelParams = {randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100)};
        PMML pmml;
        try (InputStream is = new ByteArrayInputStream(FileUtils.readFileToByteArray(FileUtils.getFile("/Users/britta/Downloads/test.xml")))) {
            Source transformedSource = ImportFilter.apply(new InputSource(is));
            pmml = JAXBUtil.unmarshalPMML(transformedSource);
        }

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
    // only just checks that nothing crashes
    // compares to mllib and fails every now and then because we do not consider the margin
    public void checkMLlibPMMLOutputValid() throws IOException, JAXBException, SAXException {
        PMML pmml;
        try (InputStream is = new ByteArrayInputStream(FileUtils.readFileToByteArray(FileUtils.getFile("/Users/britta/tmp/test.xml")))) {
            Source transformedSource = ImportFilter.apply(new InputSource(is));
            pmml = JAXBUtil.unmarshalPMML(transformedSource);
        }

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
}
