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
import org.elasticsearch.common.settings.Settings;
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
import static org.hamcrest.Matchers.instanceOf;

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


    public void testAdult() throws IOException, ExecutionException, InterruptedException {

        indexAdultData("/org/elasticsearch/script/adult.data", this);
        assertHitCount(client().prepareSearch().get(), 32560);
        indexAdultModel("/org/elasticsearch/script/lr_model_adult_full.xml");
        checkClassificationCorrect("/org/elasticsearch/script/knime_glm_adult_result.csv");
    }


    public void testSingleAdult() throws IOException, ExecutionException, InterruptedException {

        indexAdultData("/org/elasticsearch/script/singlevalueforintegtest.txt", this);
        assertHitCount(client().prepareSearch().get(), 1);
        indexAdultModel("/org/elasticsearch/script/lr_model_adult_full.xml");
        checkClassificationCorrect("/org/elasticsearch/script/singleresultforintegtest.txt");
    }


    public void testSingleAdultNotDebug() throws IOException, ExecutionException, InterruptedException {

        indexAdultData("/org/elasticsearch/script/singlevalueforintegtest.txt", this);
        assertHitCount(client().prepareSearch().get(), 1);
        indexAdultModel("/org/elasticsearch/script/naive-bayes-adult-full-r.xml");
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("debug", false);
        SearchResponse searchResponse = client().prepareSearch("test").addScriptField("pmml", new Script("1", ScriptService.ScriptType
                .STORED, PMMLModelScriptEngineService.NAME, params)).addField("_source").setSize(10000).get();
        assertSearchResponse(searchResponse);
        assertThat((String)searchResponse.getHits().getAt(0).fields().get("pmml").getValue(), instanceOf(String.class));
        assertThat((String)searchResponse.getHits().getAt(0).fields().get("pmml").getValue(), equalTo(">50K"));
    }

    private void checkClassificationCorrect(String resultFile) throws IOException {
        final String testData = copyToStringFromClasspath(resultFile);
        String resultLines[] = testData.split("\\r?\\n");
        Map<String, String> expectedResults = new HashMap<>();
        for (int i = 1; i < resultLines.length; i++) {
            expectedResults.put(Integer.toString(i), resultLines[i]);
        }
        SearchResponse searchResponse = client().prepareSearch("test").addScriptField("pmml", new Script("1", ScriptService.ScriptType
                .STORED, PMMLModelScriptEngineService.NAME, new HashMap<String, Object>())).addField("_source").setSize(10000).get();
        assertSearchResponse(searchResponse);
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            @SuppressWarnings("unchecked")
            String label = (String) ((Map<String, Object>) (hit.field("pmml").values().get(0))).get("class");
            String[] expectedResult = expectedResults.get(hit.id()).split(",");
            assertThat(label, equalTo(expectedResult[2].substring(1, expectedResult[2].length() - 1)));
        }
    }

    public static void indexAdultModel(String modelFile) throws IOException {

        final String pmmlString = copyToStringFromClasspath(modelFile);
        // create spec
        client().admin().cluster().preparePutStoredScript().setScriptLang("pmml_model").setId("1").setSource(
                jsonBuilder().startObject()
                        .field("script", pmmlString)
                        .endObject().bytes()
        ).get();
    }

    public static void indexAdultData(String data, ESIntegTestCase testCase) throws IOException, ExecutionException, InterruptedException {

        XContentBuilder mappingBuilder = jsonBuilder();
        mappingBuilder.startObject();
        mappingBuilder.startObject("type")
                .startObject("properties")
                .startObject("age")
                .field("type", "double")
                .endObject()
                .startObject("workclass")
                .field("type", "keyword")
                .endObject()
                .startObject("fnlwgt")
                .field("type", "double")
                .endObject()
                .startObject("education")
                .field("type", "keyword")
                .endObject()
                .startObject("education_num")
                .field("type", "double")
                .endObject()
                .startObject("marital_status")
                .field("type", "keyword")
                .endObject()
                .startObject("occupation")
                .field("type", "keyword")
                .endObject()
                .startObject("relationship")
                .field("type", "keyword")
                .endObject()
                .startObject("race")
                .field("type", "keyword")
                .endObject()
                .startObject("sex")
                .field("type", "keyword")
                .endObject()
                .startObject("capital_gain")
                .field("type", "double")
                .endObject()
                .startObject("capital_loss")
                .field("type", "double")
                .endObject()
                .startObject("hours_per_week")
                .field("type", "double")
                .endObject()
                .startObject("native_country")
                .field("type", "keyword")
                .endObject()
                .startObject("class")
                .field("type", "keyword")
                .endObject()

                .endObject()
                .endObject();
        mappingBuilder.endObject();
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type", mappingBuilder).get());
        final String testData = copyToStringFromClasspath(data);
        String testDataLines[] = testData.split("\\r?\\n");
        String[] fields = testDataLines[0].split(",");
        for (int i = 0; i < fields.length; i++) {
            fields[i] = fields[i].trim();
            fields[i] = fields[i].substring(1, fields[i].length() - 1);
        }
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 1; i < testDataLines.length; i++) {
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

            docs.add(client().prepareIndex("test", "type", Integer.toString(i)).setSource(input));
        }
        testCase.indexRandom(true, true, docs);
    }
}
