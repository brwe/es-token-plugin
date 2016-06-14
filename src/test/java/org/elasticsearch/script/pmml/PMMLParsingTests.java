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

import org.dmg.pmml.PMML;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.script.FieldsToVectorPMML;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.script.pmml.ProcessPMMLHelper.parsePmml;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.CoreMatchers.equalTo;

public class PMMLParsingTests extends ESTestCase {

    public void testSimplePipelineParsing() throws IOException {
        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/logistic_regression.xml");
        PMML pmml = parsePmml(pmmlString);

        PMMLModelScriptEngineService.FeaturesAndModel featuresAndModel = PMMLModelScriptEngineService.getFeaturesAndModelFromFullPMMLSpec(pmml, 0);
        assertThat(featuresAndModel.features.getEntries().size(), equalTo(15));
    }

    public void testTwoStepPipelineParsing() throws IOException {
        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/lr_model.xml");
        PMML pmml = parsePmml(pmmlString);
        PMMLModelScriptEngineService.FeaturesAndModel featuresAndModel = PMMLModelScriptEngineService.getFeaturesAndModelFromFullPMMLSpec(pmml, 0);
        FieldsToVectorPMML.FieldsToVectorPMMLGeneralizedRegression vectorEntries = (FieldsToVectorPMML.FieldsToVectorPMMLGeneralizedRegression) featuresAndModel.features;
        assertThat(vectorEntries.getEntries().size(), equalTo(3));
        assertVectorsCorrect(vectorEntries);
    }

    public void testTwoStepPipelineParsingReordered() throws IOException {
        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/lr_model_reordered.xml");
        PMML pmml = parsePmml(pmmlString);
        PMMLModelScriptEngineService.FeaturesAndModel featuresAndModel = PMMLModelScriptEngineService.getFeaturesAndModelFromFullPMMLSpec(pmml, 0);
        FieldsToVectorPMML.FieldsToVectorPMMLGeneralizedRegression vectorEntries = (FieldsToVectorPMML.FieldsToVectorPMMLGeneralizedRegression) featuresAndModel.features;
        assertThat(vectorEntries.getEntries().size(), equalTo(3));
        assertVectorsCorrect(vectorEntries);
    }

    public void assertVectorsCorrect(FieldsToVectorPMML.FieldsToVectorPMMLGeneralizedRegression vectorEntries) throws IOException {
        final String testData = copyToStringFromClasspath("/org/elasticsearch/script/test.data");
        final String expectedResults = copyToStringFromClasspath("/org/elasticsearch/script/lr_result.txt");
        String testDataLines[] = testData.split("\\r?\\n");
        String expectedResultsLines[] = expectedResults.split("\\r?\\n");
        for (int i = 0; i < testDataLines.length; i++) {
            String[] testDataValues = testDataLines[i].split(",");
            List ageInput = new ArrayList<Double>();
            ;
            if (testDataValues[0].equals("") == false) {
                ageInput.add(Double.parseDouble(testDataValues[0]));
            }
            List workInput = new ArrayList<String>();
            if (testDataValues[1].trim().equals("") == false) {
                workInput.add(testDataValues[1].trim());
            }
            Map<String, List> input = new HashMap<>();
            input.put("age", ageInput);
            input.put("work", workInput);
            Map<String, Object> result = (Map<String, Object>) vectorEntries.vector(input);
            String[] expectedResult = expectedResultsLines[i + 1].split(",");
            double expectedAgeValue = Double.parseDouble(expectedResult[0]);
            // assertThat(Double.parseDouble(expectedResult[0]), Matchers.closeTo(((double[]) result.get("values"))[0], 1.e-7));
            if (workInput.size() == 0) {
                // this might be a problem with the model. not sure. the "other" value does not appear in it.
                assertArrayEquals(((double[]) result.get("values")), new double[]{expectedAgeValue, 1.0d}, 1.e-7);
                assertArrayEquals(((int[]) result.get("indices")), new int[]{0, 4});
            } else if ("Private".equals(workInput.get(0))) {
                assertArrayEquals(((double[]) result.get("values")), new double[]{expectedAgeValue, 1.0d, 1.0d}, 1.e-7);
                assertArrayEquals(((int[]) result.get("indices")), new int[]{0, 1, 4});
            } else if ("Self-emp-inc".equals(workInput.get(0))) {
                assertArrayEquals(((double[]) result.get("values")), new double[]{expectedAgeValue, 1.0d, 1.0d}, 1.e-7);
                assertArrayEquals(((int[]) result.get("indices")), new int[]{0, 2, 4});
            } else if ("State-gov".equals(workInput.get(0))) {
                assertArrayEquals(((double[]) result.get("values")), new double[]{expectedAgeValue, 1.0d, 1.0d}, 1.e-7);
                assertArrayEquals(((int[]) result.get("indices")), new int[]{0, 3, 4});
            } else {
                fail("work input was " + workInput);
            }
        }
    }

    public void testModelAndFeatureParsing() throws IOException {
        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/lr_model.xml");
        PMML pmml = parsePmml(pmmlString);
        PMMLModelScriptEngineService.FeaturesAndModel featuresAndModel = PMMLModelScriptEngineService.getFeaturesAndModelFromFullPMMLSpec(pmml, 0);
        FieldsToVectorPMML.FieldsToVectorPMMLGeneralizedRegression vectorEntries = (FieldsToVectorPMML.FieldsToVectorPMMLGeneralizedRegression) featuresAndModel.features;
        assertThat(vectorEntries.getEntries().size(), equalTo(3));
        assertModelCorrect(featuresAndModel);
    }

    public void testBigModelAndFeatureParsing() throws IOException {
        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/lr_model_adult_full.xml");
        PMML pmml = parsePmml(pmmlString);
        PMMLModelScriptEngineService.FeaturesAndModel featuresAndModel = PMMLModelScriptEngineService.getFeaturesAndModelFromFullPMMLSpec(pmml, 0);
        FieldsToVectorPMML.FieldsToVectorPMMLGeneralizedRegression vectorEntries = (FieldsToVectorPMML.FieldsToVectorPMMLGeneralizedRegression) featuresAndModel.features;
        assertThat(vectorEntries.getEntries().size(), equalTo(15));
        assertBiggerModelCorrect(featuresAndModel, "/org/elasticsearch/script/adult.data", "/org/elasticsearch/script/knime_glm_adult_result.csv");
    }

    public void testBigModelAndFeatureParsingFromRExport() throws IOException {
        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/glm-adult-full-r.xml");
        PMML pmml = parsePmml(pmmlString);
        PMMLModelScriptEngineService.FeaturesAndModel featuresAndModel = PMMLModelScriptEngineService.getFeaturesAndModelFromFullPMMLSpec(pmml, 0);
        FieldsToVectorPMML.FieldsToVectorPMMLGeneralizedRegression vectorEntries = (FieldsToVectorPMML.FieldsToVectorPMMLGeneralizedRegression) featuresAndModel.features;
        assertThat(vectorEntries.getEntries().size(), equalTo(12));
        assertBiggerModelCorrect(featuresAndModel, "/org/elasticsearch/script/adult.data", "/org/elasticsearch/script/r_glm_adult_result" +
                ".csv");
    }

    public void testBigModelCorrectSingleValue() throws IOException {
        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/lr_model_adult_full.xml");
        PMML pmml = parsePmml(pmmlString);
        PMMLModelScriptEngineService.FeaturesAndModel featuresAndModel = PMMLModelScriptEngineService.getFeaturesAndModelFromFullPMMLSpec(pmml, 0);
        FieldsToVectorPMML.FieldsToVectorPMMLGeneralizedRegression vectorEntries = (FieldsToVectorPMML.FieldsToVectorPMMLGeneralizedRegression) featuresAndModel.features;
        assertThat(vectorEntries.getEntries().size(), equalTo(15));
        assertBiggerModelCorrect(featuresAndModel, "/org/elasticsearch/script/singlevalueforintegtest.txt", "/org/elasticsearch/script/singleresultforintegtest.txt");
    }

    private void assertModelCorrect(PMMLModelScriptEngineService.FeaturesAndModel featuresAndModel) throws IOException {
        final String testData = copyToStringFromClasspath("/org/elasticsearch/script/test.data");
        final String expectedResults = copyToStringFromClasspath("/org/elasticsearch/script/lr_result.txt");
        String testDataLines[] = testData.split("\\r?\\n");
        String expectedResultsLines[] = expectedResults.split("\\r?\\n");
        for (int i = 0; i < testDataLines.length; i++) {
            String[] testDataValues = testDataLines[i].split(",");
            List ageInput = new ArrayList<Double>();
            ;
            if (testDataValues[0].equals("") == false) {
                ageInput.add(Double.parseDouble(testDataValues[0]));
            }
            List workInput = new ArrayList<>();
            if (testDataValues[1].trim().equals("") == false) {
                workInput.add(testDataValues[1].trim());
            }
            Map<String, List> input = new HashMap<>();
            input.put("age", ageInput);
            input.put("work", workInput);
            Map<String, Object> result = (Map<String, Object>) ((FieldsToVectorPMML) featuresAndModel.features).vector(input);
            String[] expectedResult = expectedResultsLines[i + 1].split(",");
            String expectedClass = expectedResult[expectedResult.length - 1];
            expectedClass = expectedClass.substring(1, expectedClass.length() - 1);
            Map<String,Object> resultValues = featuresAndModel.getModel().evaluate(new Tuple<>((int[]) result.get("indices"), (double[]) result.get
                    ("values")));
            assertThat(expectedClass, equalTo(resultValues.get("class")));
        }
    }

    private void assertBiggerModelCorrect(PMMLModelScriptEngineService.FeaturesAndModel featuresAndModel, String inputData, String
            resultData) throws IOException {
        final String testData = copyToStringFromClasspath(inputData);
        final String expectedResults = copyToStringFromClasspath(resultData);
        String testDataLines[] = testData.split("\\r?\\n");
        String expectedResultsLines[] = expectedResults.split("\\r?\\n");
        String[] fields = testDataLines[0].split(",");
        for (int i = 0; i < fields.length; i++) {
            fields[i] = fields[i].trim();
            fields[i] = fields[i].substring(1, fields[i].length() - 1);
        }
        for (int i = 1; i < testDataLines.length; i++) {
            String[] testDataValues = testDataLines[i].split(",");
            // trimm spaces and add value
            Map<String, List> input = new HashMap<>();
            for (int j = 0; j < testDataValues.length; j++) {
                testDataValues[j] = testDataValues[j].trim();
                if (testDataValues[j].equals("") == false) {
                    List fieldInput = new ArrayList<>();
                    if (j == 0 || j == 2 || j == 4 || j == 10 || j == 11 || j == 12) {
                        fieldInput.add(Double.parseDouble(testDataValues[j]));
                    } else {
                        fieldInput.add(testDataValues[j]);
                    }
                    input.put(fields[j], fieldInput);
                } else {
                    if (randomBoolean()) {
                        input.put(fields[j], new ArrayList<>());
                    }
                }
            }
            Map<String, Object> result = (Map<String, Object>) ((FieldsToVectorPMML) featuresAndModel.features).vector(input);
            String[] expectedResult = expectedResultsLines[i].split(",");
            String expectedClass = expectedResult[expectedResult.length - 1];
            expectedClass = expectedClass.substring(1, expectedClass.length() - 1);
            Map<String,Object> resultValues = featuresAndModel.getModel().evaluate(new Tuple<>((int[]) result.get("indices"), (double[])
                    result.get
                    ("values")));
            assertThat(expectedClass, equalTo(resultValues.get("class")));
            double prob0 = (Double)((Map<String,Object>)resultValues.get("probs")).get("<=50K");
            double prob1 = (Double)((Map<String,Object>)resultValues.get("probs")).get(">50K");
            assertThat(prob0, Matchers.closeTo(Double.parseDouble(expectedResult[0]), 1.e-7));
            assertThat(prob1, Matchers.closeTo(Double.parseDouble(expectedResult[1]), 1.e-7));
        }
    }
}
