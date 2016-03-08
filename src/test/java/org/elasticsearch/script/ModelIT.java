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

import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.linalg.DenseVector;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.preparespec.PrepareSpecAction;
import org.elasticsearch.action.preparespec.PrepareSpecRequest;
import org.elasticsearch.action.preparespec.PrepareSpecResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class ModelIT extends ESIntegTestCase {


    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return pluginList(TokenPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(TokenPlugin.class);
    }

    @Test
    @AwaitsFix(bugUrl = "this hangs every now and then. needs debugging")
    public void testNaiveBayesUpdateScript() throws IOException {

        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        refresh();
        client().prepareIndex("model", "params", "test_params").setSource(
                jsonBuilder().startObject()
                        .field("pi", new double[]{1, 2})
                        .field("thetas", new double[][]{{1, 2, 3}, {3, 2, 1}})
                        .field("labels", new double[]{0, 1})
                        .field("features", new String[]{"fox", "quick", "the"})
                        .endObject()
        ).get();
        refresh();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("index", "model");
        parameters.put("type", "params");
        parameters.put("id", "test_params");
        parameters.put("field", "text");
        if (randomBoolean()) {
            parameters.put("fieldDataFields", true);
        }
        client().prepareUpdate().setId("1").setIndex("index").setType("type")
                .setScript(new Script(NaiveBayesUpdateScript.SCRIPT_NAME, ScriptService.ScriptType.INLINE, "native", parameters))
                .get();

        GetResponse getResponse = client().prepareGet().setId("1").setIndex("index").setType("type").get();
        assertNotNull(getResponse.getSource().get("label"));
        logger.info("label: {}", getResponse.getSource().get("label"));
    }


    @Test
    public void testPMMLLRDenseWithTF() throws IOException, JAXBException, SAXException, ExecutionException, InterruptedException {
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        ensureGreen("index");
        for (int i = 0; i < 10; i++) {
            double intercept = randomFloat();
            double[] modelParams = new double[]{randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100)};
            String number = "tf";
            boolean sparse = false;
            // store LR Model
            String llr = PMMLGenerator.generateLRPMMLModel(intercept, modelParams, new double[]{1, 0});
            // create spec
            client().prepareIndex("model", "pmml", "1").setSource(
                    jsonBuilder().startObject()
                            .field("pmml", llr)
                            .endObject()
            ).get();
            refresh();
            PrepareSpecResponse response = createSpecWithGivenTerms(number, sparse);
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("index", "model");
            parameters.put("type", "pmml");
            parameters.put("id", "1");
            parameters.put("spec_index", response.getIndex());
            parameters.put("spec_type", response.getType());
            parameters.put("spec_id", response.getId());
            // call PMML script with needed parameters
            SearchResponse searchResponse = client().prepareSearch("index").addScriptField("pmml", new Script(PMMLModel.SCRIPT_NAME, ScriptService.ScriptType.INLINE, "native", parameters)).get();
            assertSearchResponse(searchResponse);
            String label = (String) (searchResponse.getHits().getAt(0).field("pmml").values().get(0));
            LogisticRegressionModel lrm = new LogisticRegressionModel(new DenseVector(modelParams), intercept);
            int[] vals = new int[]{1, 2, 1, 0};
            double mllibResult = lrm.predict(new DenseVector(new double[]{vals[0], vals[1], vals[2], vals[3]}));
            assertThat(mllibResult, equalTo(Double.parseDouble(label)));
        }
    }

    @Test
    public void testPMMLLRSparseWithOccurence() throws IOException, JAXBException, SAXException, ExecutionException, InterruptedException {
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        ensureGreen("index");
        for (int i = 0; i < 10; i++) {
            double intercept = randomFloat();
            double[] modelParams = new double[]{randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100)};
            // store LR Model
            String llr = PMMLGenerator.generateLRPMMLModel(intercept, modelParams, new double[]{1, 0});
            // create spec
            client().prepareIndex("model", "pmml", "1").setSource(
                    jsonBuilder().startObject()
                            .field("pmml", llr)
                            .endObject()
            ).get();
            refresh();
            PrepareSpecResponse response = createSpecWithGivenTerms("occurence", true);
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("index", "model");
            parameters.put("type", "pmml");
            parameters.put("id", "1");
            parameters.put("spec_index", response.getIndex());
            parameters.put("spec_type", response.getType());
            parameters.put("spec_id", response.getId());
            // call PMML script with needed parameters
            SearchResponse searchResponse = client().prepareSearch("index").addScriptField("pmml", new Script(PMMLModel.SCRIPT_NAME, ScriptService.ScriptType.INLINE, "native", parameters)).get();
            assertSearchResponse(searchResponse);
            String label = (String) (searchResponse.getHits().getAt(0).field("pmml").values().get(0));
            LogisticRegressionModel lrm = new LogisticRegressionModel(new DenseVector(modelParams), intercept);
            int[] vals = new int[]{1, 1, 1, 0};
            double mllibResult = lrm.predict(new DenseVector(new double[]{vals[0], vals[1], vals[2], vals[3]}));
            assertThat(mllibResult, equalTo(Double.parseDouble(label)));
        }
    }

    @Test
    public void testPMMLSVMSparseWithTF() throws IOException, JAXBException, SAXException, ExecutionException, InterruptedException {
        createIndexWithTermVectors();
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        ensureGreen("index");
        for (int i = 0; i < 10; i++) {
            double intercept = randomFloat();
            double[] modelParams = new double[]{randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100)};
            // store LR Model
            String llr = PMMLGenerator.generateLRPMMLModel(intercept, modelParams, new double[]{1, 0});
            // create spec
            client().prepareIndex("model", "pmml", "1").setSource(
                    jsonBuilder().startObject()
                            .field("pmml", llr)
                            .endObject()
            ).get();
            refresh();
            PrepareSpecResponse response = createSpecWithGivenTerms("tf", true);
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("index", "model");
            parameters.put("type", "pmml");
            parameters.put("id", "1");
            parameters.put("spec_index", response.getIndex());
            parameters.put("spec_type", response.getType());
            parameters.put("spec_id", response.getId());
            // call PMML script with needed parameters
            SearchResponse searchResponse = client().prepareSearch("index").addScriptField("pmml", new Script(PMMLModel.SCRIPT_NAME, ScriptService.ScriptType.INLINE, "native", parameters)).get();
            assertSearchResponse(searchResponse);
            String label = (String) (searchResponse.getHits().getAt(0).field("pmml").values().get(0));
            LogisticRegressionModel lrm = new LogisticRegressionModel(new DenseVector(modelParams), intercept);
            int[] vals = new int[]{1, 2, 1, 0};
            double mllibResult = lrm.predict(new DenseVector(new double[]{vals[0], vals[1], vals[2], vals[3]}));
            assertThat(mllibResult, equalTo(Double.parseDouble(label)));
        }
    }

    @Test
    public void testPMMLSVMDenseWithTF() throws IOException, JAXBException, SAXException, ExecutionException, InterruptedException {
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        ensureGreen("index");
        for (int i = 0; i < 10; i++) {
            double intercept = randomFloat();
            double[] modelParams = new double[]{randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100)};
            String number = "tf";
            boolean sparse = false;
            // store LR Model
            String llr = PMMLGenerator.generateSVMPMMLModel(intercept, modelParams, new double[]{1, 0});
            // create spec
            client().prepareIndex("model", "pmml", "1").setSource(
                    jsonBuilder().startObject()
                            .field("pmml", llr)
                            .endObject()
            ).get();
            refresh();
            PrepareSpecResponse response = createSpecWithGivenTerms(number, sparse);
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("index", "model");
            parameters.put("type", "pmml");
            parameters.put("id", "1");
            parameters.put("spec_index", response.getIndex());
            parameters.put("spec_type", response.getType());
            parameters.put("spec_id", response.getId());
            // call PMML script with needed parameters
            SearchResponse searchResponse = client().prepareSearch("index").addScriptField("pmml", new Script(PMMLModel.SCRIPT_NAME, ScriptService.ScriptType.INLINE, "native", parameters)).get();
            assertSearchResponse(searchResponse);
            String label = (String) (searchResponse.getHits().getAt(0).field("pmml").values().get(0));
            SVMModel svmm = new SVMModel(new DenseVector(modelParams), intercept);
            int[] vals = new int[]{1, 2, 1, 0};
            double mllibResult = svmm.predict(new DenseVector(new double[]{vals[0], vals[1], vals[2], vals[3]}));
            assertThat(mllibResult, equalTo(Double.parseDouble(label)));
        }
    }

    @Test
    public void testPMMLSVMSparseWithOccurence() throws IOException, JAXBException, SAXException, ExecutionException, InterruptedException {
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        ensureGreen("index");
        for (int i = 0; i < 10; i++) {
            double intercept = randomFloat();
            double[] modelParams = new double[]{randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100)};
            // store LR Model
            String llr = PMMLGenerator.generateSVMPMMLModel(intercept, modelParams, new double[]{1, 0});
            // create spec
            client().prepareIndex("model", "pmml", "1").setSource(
                    jsonBuilder().startObject()
                            .field("pmml", llr)
                            .endObject()
            ).get();
            refresh();
            PrepareSpecResponse response = createSpecWithGivenTerms("occurence", true);
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("index", "model");
            parameters.put("type", "pmml");
            parameters.put("id", "1");
            parameters.put("spec_index", response.getIndex());
            parameters.put("spec_type", response.getType());
            parameters.put("spec_id", response.getId());
            // call PMML script with needed parameters
            SearchResponse searchResponse = client().prepareSearch("index").addScriptField("pmml", new Script(PMMLModel.SCRIPT_NAME, ScriptService.ScriptType.INLINE, "native", parameters)).get();
            assertSearchResponse(searchResponse);
            String label = (String) (searchResponse.getHits().getAt(0).field("pmml").values().get(0));
            SVMModel svmm = new SVMModel(new DenseVector(modelParams), intercept);
            int[] vals = new int[]{1, 1, 1, 0};
            double mllibResult = svmm.predict(new DenseVector(new double[]{vals[0], vals[1], vals[2], vals[3]}));
            assertThat(mllibResult, equalTo(Double.parseDouble(label)));
        }
    }

    @Test
    public void testPMMLLRSparseWithTF() throws IOException, JAXBException, SAXException, ExecutionException, InterruptedException {
        createIndexWithTermVectors();
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        ensureGreen("index");
        for (int i = 0; i < 10; i++) {
            double intercept = randomFloat();
            double[] modelParams = new double[]{randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100)};
            // store LR Model
            String llr = PMMLGenerator.generateSVMPMMLModel(intercept, modelParams, new double[]{1, 0});
            // create spec
            client().prepareIndex("model", "pmml", "1").setSource(
                    jsonBuilder().startObject()
                            .field("pmml", llr)
                            .endObject()
            ).get();
            refresh();
            PrepareSpecResponse response = createSpecWithGivenTerms("tf", true);
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("index", "model");
            parameters.put("type", "pmml");
            parameters.put("id", "1");
            parameters.put("spec_index", response.getIndex());
            parameters.put("spec_type", response.getType());
            parameters.put("spec_id", response.getId());
            // call PMML script with needed parameters
            SearchResponse searchResponse = client().prepareSearch("index").addScriptField("pmml", new Script(PMMLModel.SCRIPT_NAME, ScriptService.ScriptType.INLINE, "native", parameters)).get();
            assertSearchResponse(searchResponse);
            String label = (String) (searchResponse.getHits().getAt(0).field("pmml").values().get(0));
            SVMModel svmm = new SVMModel(new DenseVector(modelParams), intercept);
            int[] vals = new int[]{1, 2, 1, 0};
            double mllibResult = svmm.predict(new DenseVector(new double[]{vals[0], vals[1], vals[2], vals[3]}));
            assertThat(mllibResult, equalTo(Double.parseDouble(label)));
        }
    }

    public PrepareSpecResponse createSpecWithGivenTerms(String number, boolean sparse) throws IOException, InterruptedException, ExecutionException {
        XContentBuilder source = jsonBuilder();
        source.startObject()
                .startArray("features")
                .startObject()
                .field("field", "text")
                .field("tokens", "given")
                .field("terms", new String[]{"fox", "quick", "the", "zonk"})
                .field("number", number)
                .field("type", "string")
                .endObject()
                .endArray()
                .field("sparse", sparse)
                .endObject();
        return client().execute(PrepareSpecAction.INSTANCE, new PrepareSpecRequest().source(source.string())).get();
    }

    void createIndexWithTermVectors() throws IOException {
        XContentBuilder mapping = jsonBuilder();
        mapping.startObject()
                .startObject("type")
                .startObject("properties")
                .startObject("text")
                .field("type", "string")
                .field("term_vector", "yes")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        client().admin().indices().prepareCreate("index").addMapping("type", mapping).get();
    }
}
