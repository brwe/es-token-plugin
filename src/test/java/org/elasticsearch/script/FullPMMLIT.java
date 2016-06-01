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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.pmml.PMMLModelScriptEngineService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class FullPMMLIT extends ESIntegTestCase {


    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return pluginList(TokenPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(TokenPlugin.class);
    }

    @Test
    public void testAdult() throws IOException, ExecutionException, InterruptedException {

        indexAdultData("/org/elasticsearch/script/adult.data", "/org/elasticsearch/script/lr_result_adult_full.txt");
        assertHitCount(client().prepareSearch().get(), 32561);
        indexAdultModel();
        checkClassificationCorrect();
    }

    @Test
    public void testSingleAdult() throws IOException, ExecutionException, InterruptedException {

        indexAdultData("/org/elasticsearch/script/singlevalueforintegtest.txt", "/org/elasticsearch/script/singleresultforintegtest.txt");
        assertHitCount(client().prepareSearch().get(), 1);
        indexAdultModel();
        checkClassificationCorrect();
    }

    private void checkClassificationCorrect() {
        SearchResponse searchResponse = client().prepareSearch("test").addScriptField("pmml", new Script("1", ScriptService.ScriptType
                .INDEXED, PMMLModelScriptEngineService.NAME, new HashMap<String, Object>())).addField("_source").setSize(10000).get();
        assertSearchResponse(searchResponse);
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            String label = (String) (hit.field("pmml").values().get(0));
            String predictedLabel = (String) (hit.sourceAsMap().get("expected_model_prediction"));
            assertThat(label, equalTo(predictedLabel));
        }
    }

    private void indexAdultModel() throws IOException {

        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/lr_model_adult_full.xml");
        // create spec
        client().prepareIndex(ScriptService.SCRIPT_INDEX, "pmml_model", "1").setSource(
                jsonBuilder().startObject()
                        .field("script", pmmlString)
                        .endObject()
        ).get();
    }

    private void indexAdultData(String data, String result) throws IOException, ExecutionException, InterruptedException {

        XContentBuilder mappingBuilder = jsonBuilder();
        mappingBuilder.startObject();
        mappingBuilder.startObject("type")
                .startObject("properties")
                .startObject("age")
                .field("type", "double")
                .endObject()
                .startObject("workclass")
                .field("type", "string")
                .field("analyzer", "keyword")
                .endObject()
                .startObject("fnlwgt")
                .field("type", "double")
                .endObject()
                .startObject("education")
                .field("type", "string")
                .field("analyzer", "keyword")
                .endObject()
                .startObject("education-num")
                .field("type", "double")
                .endObject()
                .startObject("marital-status")
                .field("type", "string")
                .field("analyzer", "keyword")
                .endObject()
                .startObject("occupation")
                .field("type", "string")
                .field("analyzer", "keyword")
                .endObject()
                .startObject("relationship")
                .field("type", "string")
                .field("analyzer", "keyword")
                .endObject()
                .startObject("race")
                .field("type", "string")
                .field("analyzer", "keyword")
                .endObject()
                .startObject("sex")
                .field("type", "string")
                .field("analyzer", "keyword")
                .endObject()
                .startObject("capital-gain")
                .field("type", "double")
                .endObject()
                .startObject("capital-loss")
                .field("type", "double")
                .endObject()
                .startObject("hours-per-week")
                .field("type", "double")
                .endObject()
                .startObject("native-country")
                .field("type", "string")
                .field("analyzer", "keyword")
                .endObject()
                .startObject("class")
                .field("type", "string")
                .field("analyzer", "keyword")
                .endObject()

                .endObject()
                .endObject();
        mappingBuilder.endObject();
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type", mappingBuilder).get());
        final String testData = copyToStringFromClasspath(data);
        final String expectedResults = copyToStringFromClasspath(result);
        String testDataLines[] = testData.split("\\r?\\n");
        String expectedResultsLines[] = expectedResults.split("\\r?\\n");
        String[] fields = expectedResultsLines[0].split(",");
        for (int i = 0; i < fields.length; i++) {
            fields[i] = fields[i].trim();
            fields[i] = fields[i].substring(1, fields[i].length() - 1);
        }
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < testDataLines.length; i++) {
            String[] testDataValues = testDataLines[i].split(",");
            // trimm spaces and add value
            Map<String, Object> input = new HashMap<>();
            for (int j = 0; j < testDataValues.length; j++) {
                testDataValues[j] = testDataValues[j].trim();
                if (testDataValues[j].equals("") == false) {
                    input.put(fields[j], testDataValues[j]);
                } else {
                    if (randomBoolean()) {
                        input.put(fields[j], null);
                    }
                }
            }
            // get the class that the lr_model predicted
            String[] expectedResultLine = expectedResultsLines[i + 1].split(",");
            String expectedPrediction = expectedResultLine[expectedResultLine.length - 1];
            expectedPrediction = expectedPrediction.substring(1, expectedPrediction.length() - 1);
            input.put("expected_model_prediction", expectedPrediction);
            input.remove("class");
            docs.add(client().prepareIndex("test", "type", Integer.toString(i)).setSource(input));
        }
        indexRandom(true, true, docs);
    }
}
