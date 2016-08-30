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

package org.elasticsearch.search.fetch.termvectors;

import org.elasticsearch.action.preparespec.PrepareSpecRequestBuilder;
import org.elasticsearch.action.preparespec.PrepareSpecResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.action.preparespec.PrepareSpecTests.getTextFieldRequestSourceWithAllTerms;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;


public class TermVectorsFetchIT extends ESIntegTestCase {

    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(TokenPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TokenPlugin.class);
    }

    @SuppressWarnings("unchecked")
    public void testSimpleFetchTermvectors() throws IOException {

        client().admin()
                .indices()
                .prepareCreate("test")
                .addMapping(
                        "type",
                        jsonBuilder()
                                .startObject().startObject("type")
                                .startObject("properties")
                                .startObject("test")
                                .field("type", "text").field("term_vector", "yes")
                                .endObject()
                                .endObject()
                                .endObject().endObject()).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        client().index(
                indexRequest("test").type("type").id("1")
                        .source(jsonBuilder().startObject().field("test", "I am sam i am").endObject())).actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchSourceBuilder searchSource = SearchSourceBuilder.searchSource().ext(
                jsonBuilder().startObject()
                .startObject(TermVectorsFetchSubPhase.NAMES[0])
                .field("fields", new String[]{"test"})
                .endObject()
                .endObject());
        SearchResponse response = client().prepareSearch().setSource(searchSource).get();
        assertSearchResponse(response);
        logger.info(response.toString());
        SearchHit hit = response.getHits().getAt(0);
        // get the fields from the response
        SearchHitField fields = hit.field(TermVectorsFetchSubPhase.NAMES[0]);
        Map<String, Object> termVectors = fields.getValue();
        // get frequencies for field test
        Map<String, Object> field = (Map<String, Object>) termVectors.get("test");
        Map<String, Object> freqs = (Map<String, Object>) field.get("terms");
        assertThat((Integer) ((Map<String, Object>) freqs.get("i")).get("term_freq"), equalTo(2));
        assertThat((Integer) ((Map<String, Object>) freqs.get("am")).get("term_freq"), equalTo(2));
        assertThat((Integer) ((Map<String, Object>) freqs.get("sam")).get("term_freq"), equalTo(1));
    }

    @SuppressWarnings("unchecked")
    public void testFetchTermvectorsAndFieldsWork() throws IOException {

        client().admin()
                .indices()
                .prepareCreate("test")
                .addMapping(
                        "type",
                        jsonBuilder()
                                .startObject().startObject("type")
                                .startObject("properties")
                                .startObject("text")
                                .field("type", "text").field("term_vector", "yes").field("store", "yes")
                                .endObject()
                                .endObject()
                                .endObject().endObject()).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        client().index(
                indexRequest("test").type("type").id("1")
                        .source(jsonBuilder().startObject().field("text", "I am sam i am").endObject())).actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchSourceBuilder searchSource = SearchSourceBuilder.searchSource().ext(
                jsonBuilder().startObject()
                .startObject(TermVectorsFetchSubPhase.NAMES[0])
                .field("fields", new String[]{"text"})
                .endObject()
                .endObject())
                .storedField("text");
        SearchResponse response = client().prepareSearch().setSource(searchSource).get();
        assertSearchResponse(response);
        logger.info(response.toString());
        SearchHit hit = response.getHits().getAt(0);
        // get the fields from the response
        SearchHitField fields = hit.field(TermVectorsFetchSubPhase.NAMES[0]);
        Map<String, Object> termVectors = fields.getValue();
        // get frequencies for field test
        Map<String, Object> field = (Map<String, Object>) termVectors.get("text");
        Map<String, Object> freqs = (Map<String, Object>) field.get("terms");
        assertThat((Integer) ((Map<String, Object>) freqs.get("i")).get("term_freq"), equalTo(2));
        assertThat((Integer) ((Map<String, Object>) freqs.get("am")).get("term_freq"), equalTo(2));
        assertThat((Integer) ((Map<String, Object>) freqs.get("sam")).get("term_freq"), equalTo(1));
        SearchHitField textField = hit.field("text");
        assertThat((String) textField.getValue(), equalTo("I am sam i am"));
    }

    @SuppressWarnings("unchecked")
    public void testFetchTermvectorsAndScriptFieldsWork() throws IOException {

        client().admin()
                .indices()
                .prepareCreate("index")
                .setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1))
                .addMapping(
                        "type",
                        jsonBuilder()
                                .startObject().startObject("type")
                                .startObject("properties")
                                .startObject("text")
                                .field("type", "text").field("term_vector", "yes").field("store", "yes")
                                .endObject()
                                .endObject()
                                .endObject().endObject()).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        client().index(
                indexRequest("index").type("type").id("1")
                        .source(jsonBuilder().startObject().field("text", "I am sam i am").endObject())).actionGet();
        client().index(
                indexRequest("index").type("type").id("2")
                        .source(jsonBuilder().startObject().field("text", "I am sam i am").endObject())).actionGet();

        ensureGreen();
        refresh();
        PrepareSpecResponse prepareSpecResponse = new PrepareSpecRequestBuilder(client())
                .source(getTextFieldRequestSourceWithAllTerms().string()).setId("my_id").get();
        Map<String, Object> params = new HashMap<>();
        params.put("spec", prepareSpecResponse.getSpecAsMap());
        SearchSourceBuilder searchSource = SearchSourceBuilder.searchSource().ext(
                jsonBuilder().startObject()
                .startObject(TermVectorsFetchSubPhase.NAMES[0])
                .field("fields", new String[]{"text"})
                .endObject()
                .endObject())
                .scriptField("vectors", new Script("doc_to_vector", ScriptService.ScriptType.INLINE, "native", params))
                .storedField("text");
        SearchResponse response = client().prepareSearch().setSource(searchSource).get();
        assertSearchResponse(response);
        logger.info(response.toString());
        SearchHit hit = response.getHits().getAt(0);
        // get the fields from the response
        SearchHitField fields = hit.field(TermVectorsFetchSubPhase.NAMES[0]);
        Map<String, Object> termVectors = fields.getValue();
        // get frequencies for field test
        Map<String, Object> field = (Map<String, Object>) termVectors.get("text");
        Map<String, Object> freqs = (Map<String, Object>) field.get("terms");
        assertThat((Integer) ((Map<String, Object>) freqs.get("i")).get("term_freq"), equalTo(2));
        assertThat((Integer) ((Map<String, Object>) freqs.get("am")).get("term_freq"), equalTo(2));
        assertThat((Integer) ((Map<String, Object>) freqs.get("sam")).get("term_freq"), equalTo(1));
        SearchHitField textField = hit.field("text");
        assertThat((String) textField.getValue(), equalTo("I am sam i am"));
        SearchHitField vector = hit.field("vectors");
        Map<String, Object> vectorAsMap = (Map<String, Object>) vector.getValue();
        assertArrayEquals((double[]) vectorAsMap.get("values"), new double[]{2, 2, 1}, 0.0);

    }

    @SuppressWarnings("unchecked")
    public void testFetchTermvectorsAndCustomAnalyzerWorks() throws IOException {

        client().admin()
                .indices()
                .prepareCreate("index")
                .setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1))
                .addMapping(
                        "type",
                        jsonBuilder()
                                .startObject().startObject("type")
                                .startObject("properties")
                                .startObject("text")
                                .field("type", "text").field("term_vector", "yes")
                                .endObject()
                                .endObject()
                                .endObject().endObject()).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        client().index(
                indexRequest("index").type("type").id("1")
                        .source(jsonBuilder().startObject().field("text", "I am sam i am").endObject())).actionGet();
        client().index(
                indexRequest("index").type("type").id("2")
                        .source(jsonBuilder().startObject().field("text", "I am sam i am").endObject())).actionGet();

        ensureGreen();
        refresh();
        SearchSourceBuilder searchSource = SearchSourceBuilder.searchSource().ext(
                jsonBuilder().startObject()
                .startObject(TermVectorsFetchSubPhase.NAMES[0])
                .startObject("per_field_analyzer")
                .field("text", "keyword")
                .endObject()
                .endObject()
                .endObject());
        SearchResponse response = client().prepareSearch().setSource(searchSource).get();
        assertSearchResponse(response);
        logger.info(response.toString());
        SearchHit hit = response.getHits().getAt(0);
        // get the fields from the response
        SearchHitField fields = hit.field(TermVectorsFetchSubPhase.NAMES[0]);
        Map<String, Object> termVectors = fields.getValue();
        // get frequencies for field test
        Map<String, Object> field = (Map<String, Object>) termVectors.get("text");
        Map<String, Object> freqs = (Map<String, Object>) field.get("terms");
        assertThat((Integer) ((Map<String, Object>) freqs.get("I am sam i am")).get("term_freq"), equalTo(1));


    }
}
