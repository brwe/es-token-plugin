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

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.preparespec.PrepareSpecAction;
import org.elasticsearch.action.preparespec.PrepareSpecRequest;
import org.elasticsearch.action.preparespec.PrepareSpecResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.pmml.PMMLModelScriptEngineService;
import org.elasticsearch.script.pmml.PMMLVectorScriptEngineService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

/**
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class VectorIT extends ESIntegTestCase {

    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return pluginList(TokenPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(TokenPlugin.class);
    }

    @Test
    public void testVectorScript() throws IOException, ExecutionException, InterruptedException {
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        ensureGreen("index");
        refresh();
        XContentBuilder source = jsonBuilder();
        source.startObject()
                .startArray("features")
                .startObject()
                .field("field", "text")
                .field("tokens", "given")
                .field("terms", new String[]{"fox", "quick", "the"})
                .field("number", "tf")
                .field("type", "string")
                .endObject()
                .endArray()
                .field("sparse", false)
                .endObject();
        PrepareSpecResponse specResponse = client().execute(PrepareSpecAction.INSTANCE, new PrepareSpecRequest(source.string())).get();

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("spec_index", specResponse.getIndex());
        parameters.put("spec_type", specResponse.getType());
        parameters.put("spec_id", specResponse.getId());
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("vector", new Script(specResponse.getId(), ScriptService.ScriptType.INDEXED, PMMLVectorScriptEngineService.NAME, new HashMap<String, Object>())).get();
        assertSearchResponse(searchResponse);
        Map<String, Object> vector = (Map<String, Object>) (searchResponse.getHits().getAt(0).field("vector").values().get(0));
        double[] values = (double[]) vector.get("values");
        assertThat(values.length, equalTo(3));
        assertThat(values[0], equalTo(1.0));
        assertThat(values[1], equalTo(2.0));
        assertThat(values[2], equalTo(1.0));

    }

    @Test
    public void testSparseVectorScript() throws IOException, ExecutionException, InterruptedException {
        createIndexWithTermVectors();
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        ensureGreen("index");
        refresh();
        XContentBuilder source = jsonBuilder();
        source.startObject()
                .startArray("features")
                .startObject()
                .field("field", "text")
                .field("tokens", "given")
                .field("terms", new String[]{"fox", "lame", "quick", "the"})
                .field("number", "tf")
                .field("type", "string")
                .endObject()
                .endArray()
                .field("sparse", true)
                .endObject();
        PrepareSpecResponse specResponse = client().execute(PrepareSpecAction.INSTANCE, new PrepareSpecRequest(source.string())).get();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("spec_index", specResponse.getIndex());
        parameters.put("spec_type", specResponse.getType());
        parameters.put("spec_id", specResponse.getId());
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("vector", new Script(specResponse.getId(), ScriptService.ScriptType.INDEXED, PMMLVectorScriptEngineService.NAME, new HashMap<String, Object>())).get();
        assertSearchResponse(searchResponse);
        Map<String, Object> vector = (Map<String, Object>) (searchResponse.getHits().getAt(0).field("vector").values().get(0));
        double[] values = (double[]) vector.get("values");
        assertThat(values.length, equalTo(3));
        assertThat(values[0], equalTo(1.0));
        assertThat(values[1], equalTo(2.0));
        assertThat(values[2], equalTo(1.0));
        int[] indices = (int[]) vector.get("indices");
        assertThat(indices.length, equalTo(3));
        assertThat(indices[0], equalTo(0));
        assertThat(indices[1], equalTo(2));
        assertThat(indices[2], equalTo(3));
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

    @Test
    public void testVectorScriptWithSignificantTermsSortsTerms() throws IOException, ExecutionException, InterruptedException {

        client().admin().indices().prepareCreate("index").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1));
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "I have to get up at 4am", "label", "negative").get();
        client().prepareIndex().setId("2").setIndex("index").setType("type").setSource("text", "I need to get up at 5am", "label", "negative").get();
        client().prepareIndex().setId("3").setIndex("index").setType("type").setSource("text", "I have to get up at 6am already", "label", "negative").get();
        client().prepareIndex().setId("4").setIndex("index").setType("type").setSource("text", "I need to get up at 7am", "label", "negative").get();
        client().prepareIndex().setId("5").setIndex("index").setType("type").setSource("text", "I got up at 8am", "label", "negative").get();
        client().prepareIndex().setId("6").setIndex("index").setType("type").setSource("text", "I could sleep until 9am", "label", "positive").get();
        client().prepareIndex().setId("7").setIndex("index").setType("type").setSource("text", "I only got up at 10am", "label", "positive").get();
        client().prepareIndex().setId("8").setIndex("index").setType("type").setSource("text", "I slept until 11am", "label", "positive").get();
        client().prepareIndex().setId("9").setIndex("index").setType("type").setSource("text", "I dragged myself out of bed at 12am", "label", "negative").get();
        client().prepareIndex().setId("10").setIndex("index").setType("type").setSource("text", "Damn! I missed the alarm clock and got up at 1pm. Hope Clinton does not notice...", "label", "negative").get();
        client().prepareIndex().setId("11").setIndex("index").setType("type").setSource("text", "I fell asleep at 8pm already", "label", "positive").get();
        client().prepareIndex().setId("12").setIndex("index").setType("type").setSource("text", "I fell asleep at 9pm already", "label", "positive").get();
        client().prepareIndex().setId("13").setIndex("index").setType("type").setSource("text", "I fell asleep at 10pm already", "label", "positive").get();

        ensureGreen("index");
        refresh();

        PrepareSpecResponse specResponse = client().execute(PrepareSpecAction.INSTANCE, new PrepareSpecRequest(getTextFieldRequestSourceWithSignificnatTerms().string())).get();
        GetResponse spec = client().prepareGet(specResponse.getIndex(), specResponse.getType(), specResponse.getId()).get();

        ArrayList<Map<String, Object>> features = (ArrayList<Map<String, Object>>)SharedMethods.getSourceAsMap((String)spec.getSource().get("script")).get("features");
        String lastTerm = "";
        for (String term : (ArrayList<String>) features.get(0).get("terms")) {
            assertThat(lastTerm.compareTo(term), lessThan(0));
            lastTerm = term;
        }
    }

    protected static XContentBuilder getTextFieldRequestSourceWithSignificnatTerms() throws IOException {
        XContentBuilder source = jsonBuilder();
        XContentBuilder request = jsonBuilder();

        request.startObject()
                .startObject("aggregations")
                .startObject("classes")
                .startObject("terms")
                .field("field", "label")
                .field("min_doc_count", 0)
                .field("shard_min_doc_count", 0)
                .endObject()
                .startObject("aggregations")
                .startObject("tokens")
                .startObject("significant_terms")
                .field("field", "text")
                .field("min_doc_count", 0)
                .field("size", 100)
                .field("shard_min_doc_count", 0)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        source.startObject()
                .startArray("features")
                .startObject()
                .field("type", "string")
                .field("field", "text")
                .field("tokens", "significant_terms")
                .field("request", request.string())
                .field("index", "index")
                .field("number", "tf")
                .endObject()
                .endArray()
                .field("sparse", false)
                .endObject();
        return source;
    }

    @Test
    public void testVectorScriptWithGivenTermsSortsTerms() throws IOException, ExecutionException, InterruptedException {

        PrepareSpecResponse specResponse = client().execute(PrepareSpecAction.INSTANCE, new PrepareSpecRequest(getTextFieldRequestSourceWithGivenTerms().string())).get();
        GetResponse spec = client().prepareGet(specResponse.getIndex(), specResponse.getType(), specResponse.getId()).get();

        ArrayList<Map<String, Object>> features = (ArrayList<Map<String, Object>>)SharedMethods.getSourceAsMap((String)spec.getSource().get("script")).get("features");
        String lastTerm = "";
        for (String term : (ArrayList<String>) features.get(0).get("terms")) {
            assertThat(lastTerm.compareTo(term), lessThan(0));
            lastTerm = term;
        }
    }

    private XContentBuilder getTextFieldRequestSourceWithGivenTerms() throws IOException {
        XContentBuilder source = jsonBuilder();
        source.startObject()
                .startArray("features")
                .startObject()
                .field("type", "string")
                .field("field", "text")
                .field("tokens", "given")
                .field("terms", new String[]{"these", "terms", "are", "not", "ordered"})
                .field("number", "tf")
                .endObject()
                .endArray()
                .field("sparse", false)
                .endObject();
        return source;
    }

  /*  @Test
    public void testWithScroll() throws IOException, ExecutionException, InterruptedException {
        createIndexWithTermVectors();
        for (int i = 0; i< 1000; i++) {
            client().prepareIndex().setIndex("test").setType("type").setSource("text", "a b c").get();
        }
        refresh();
        PrepareSpecResponse specResponse = client().execute(PrepareSpecAction.INSTANCE, new PrepareSpecRequest(getTextFieldRequestSourceWithGivenTerms().string())).get();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("spec_index", specResponse.getIndex());
        parameters.put("spec_type", specResponse.getType());
        parameters.put("spec_id", specResponse.getId());
        SearchResponse searchResponse = client().prepareSearch("test").addScriptField("vector", new Script("vector", ScriptService.ScriptType.INLINE, "native", parameters)).setScroll("10m").setSize(10).get();

        assertSearchResponse(searchResponse);
        searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll("10m").get();
        while(searchResponse.getHits().hits().length>0) {
            logger.info("next scroll request...");
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll("10m").get();
        }
        client().prepareClearScroll().addScrollId(searchResponse.getScrollId()).get();
    }*/

}
