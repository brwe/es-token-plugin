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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

/**
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class VectorIT extends ESIntegTestCase {

    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(TokenPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TokenPlugin.class);
    }

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

        Map<String, Object> params = new HashMap<>();
        params.put("spec", specResponse.getSpecAsMap());
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("vector", new Script("doc_to_vector",
                ScriptService.ScriptType.INLINE, "native", params)).get();
        assertSearchResponse(searchResponse);
        @SuppressWarnings("unchecked")
        Map<String, Object> vector = (Map<String, Object>) (searchResponse.getHits().getAt(0).field("vector").values().get(0));
        double[] values = (double[]) vector.get("values");
        assertThat(values.length, equalTo(3));
        assertThat(values[0], equalTo(1.0));
        assertThat(values[1], equalTo(2.0));
        assertThat(values[2], equalTo(1.0));

    }

    public void testVectorScriptSparseOccurence() throws IOException, ExecutionException, InterruptedException {
        client().admin().indices().prepareCreate("index").setSettings().addMapping("type", getMapping()).get();
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        ensureGreen("index");
        refresh();
        XContentBuilder source = jsonBuilder();
        source.startObject()
                .startArray("features")
                .startObject()
                .field("field", "text")
                .field("tokens", "given")
                .field("terms", new String[]{"fox", "quick", "the", "zonk"})
                .field("number", "occurrence")
                .field("type", "string")
                .endObject()
                .endArray()
                .field("sparse", true)
                .endObject();
        PrepareSpecResponse specResponse = client().execute(PrepareSpecAction.INSTANCE, new PrepareSpecRequest(source.string())).get();

        Map<String, Object> params = new HashMap<>();
        params.put("spec", specResponse.getSpecAsMap());
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("vector", new Script("doc_to_vector",
                ScriptService.ScriptType.INLINE, "native", params)).get();
        assertSearchResponse(searchResponse);
        @SuppressWarnings("unchecked")
        Map<String, Object> vector = (Map<String, Object>) (searchResponse.getHits().getAt(0).field("vector").values().get(0));
        double[] values = (double[]) vector.get("values");
        assertThat(values.length, equalTo(3));
        assertThat(values[0], equalTo(1.0));
        assertThat(values[1], equalTo(1.0));
        assertThat(values[2], equalTo(1.0));

        int[] indices = (int[]) vector.get("indices");
        assertThat(indices.length, equalTo(3));
        assertThat(indices[0], equalTo(0));
        assertThat(indices[1], equalTo(1));
        assertThat(indices[2], equalTo(2));

    }

    public void testVectorScriptDenseOccurence() throws IOException, ExecutionException, InterruptedException {
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        ensureGreen("index");
        refresh();
        XContentBuilder source = jsonBuilder();
        source.startObject()
                .startArray("features")
                .startObject()
                .field("field", "text")
                .field("tokens", "given")
                .field("terms", new String[]{"fox", "quick", "the", "zonk"})
                .field("number", "occurrence")
                .field("type", "string")
                .endObject()
                .endArray()
                .field("sparse", false)
                .endObject();
        PrepareSpecResponse specResponse = client().execute(PrepareSpecAction.INSTANCE, new PrepareSpecRequest(source.string())).get();

        Map<String, Object> params = new HashMap<>();
        params.put("spec", specResponse.getSpecAsMap());
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("vector", new Script("doc_to_vector",
                ScriptService.ScriptType.INLINE, "native", params)).get();
        assertSearchResponse(searchResponse);
        @SuppressWarnings("unchecked")
        Map<String, Object> vector = (Map<String, Object>) (searchResponse.getHits().getAt(0).field("vector").values().get(0));
        double[] values = (double[]) vector.get("values");
        assertThat(values.length, equalTo(4));
        assertThat(values[0], equalTo(1.0));
        assertThat(values[1], equalTo(1.0));
        assertThat(values[2], equalTo(1.0));
        assertThat(values[3], equalTo(0.0));

    }

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
        Map<String, Object> params = new HashMap<>();
        params.put("spec", specResponse.getSpecAsMap());
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("vector", new Script("doc_to_vector",
                ScriptService.ScriptType.INLINE, "native", params)).get();
        assertSearchResponse(searchResponse);
        @SuppressWarnings("unchecked")
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
        int length = (int) vector.get("length");
        assertThat(length, equalTo(4));
    }


    @AwaitsFix(bugUrl = "Must fix Index lookup first")
    public void testSparseVectorScriptWithTFWithoutTermVectorsStored() throws IOException, ExecutionException, InterruptedException {
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
        Map<String, Object> params = new HashMap<>();
        params.put("spec", specResponse.getSpecAsMap());
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("vector", new Script("doc_to_vector",
                ScriptService.ScriptType.INLINE, "native", params)).get();
        assertSearchResponse(searchResponse);
        @SuppressWarnings("unchecked")
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

    @SuppressWarnings("unchecked")
    public void testSparseVectorWithIDF() throws IOException, ExecutionException, InterruptedException {
        createIndexWithTermVectors();
        indexRandom(true,
                client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick"),
                client().prepareIndex().setId("2").setIndex("index").setType("type").setSource("text", "the quick fox is brown"),
                client().prepareIndex().setId("3").setIndex("index").setType("type").setSource("text", "the brown fox is lame"),
                client().prepareIndex().setId("4").setIndex("index").setType("type").setSource("text", "the zonk is quick"));
        ensureGreen("index");
        refresh();
        XContentBuilder source = jsonBuilder();
        source.startObject()
                .startArray("features")
                .startObject()
                .field("field", "text")
                .field("tokens", "given")
                .field("terms", new String[]{"fox", "lame", "quick", "the", "zonk"})
                .field("number", "tf_idf")
                .field("type", "string")
                .endObject()
                .endArray()
                .field("sparse", true)
                .endObject();
        PrepareSpecResponse specResponse = client().execute(PrepareSpecAction.INSTANCE, new PrepareSpecRequest(source.string())).get();
        Map<String, Object> params = new HashMap<>();
        params.put("spec", specResponse.getSpecAsMap());
        SearchResponse searchResponse = client().prepareSearch("index").addSort("_uid", SortOrder.ASC).addScriptField("vector", new
                Script("doc_to_vector", ScriptService.ScriptType.INLINE, "native", params)).get();
        assertSearchResponse(searchResponse);

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        Map<String, Object> vector = (Map<String, Object>) (searchResponse.getHits().getAt(0).field("vector").values().get(0));
        double[] values = (double[]) vector.get("values");
        assertThat(values.length, equalTo(3));
        assertThat(values[0], equalTo(0.22314355131420976));
        assertThat(values[1], equalTo(0.44628710262841953));
        assertThat(values[2], equalTo(0.0));
        int[] indices = (int[]) vector.get("indices");
        assertThat(indices.length, equalTo(3));
        assertThat(indices[0], equalTo(0));
        assertThat(indices[1], equalTo(2));
        assertThat(indices[2], equalTo(3));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        vector = (Map<String, Object>) (searchResponse.getHits().getAt(1).field("vector").values().get(0));
        values = (double[]) vector.get("values");
        assertThat(values.length, equalTo(3));
        assertThat(values[0], equalTo(0.22314355131420976));
        assertThat(values[1], equalTo(0.22314355131420976));
        assertThat(values[2], equalTo(0.0));
        indices = (int[]) vector.get("indices");
        assertThat(indices.length, equalTo(3));
        assertThat(indices[0], equalTo(0));
        assertThat(indices[1], equalTo(2));
        assertThat(indices[2], equalTo(3));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
        vector = (Map<String, Object>) (searchResponse.getHits().getAt(2).field("vector").values().get(0));
        values = (double[]) vector.get("values");
        assertThat(values.length, equalTo(3));
        assertThat(values[0], equalTo(0.22314355131420976));
        assertThat(values[1], equalTo(0.9162907318741551));
        assertThat(values[2], equalTo(0.0));
        indices = (int[]) vector.get("indices");
        assertThat(indices.length, equalTo(3));
        assertThat(indices[0], equalTo(0));
        assertThat(indices[1], equalTo(1));
        assertThat(indices[2], equalTo(3));

        assertThat(searchResponse.getHits().getAt(3).getId(), equalTo("4"));
        vector = (Map<String, Object>) (searchResponse.getHits().getAt(3).field("vector").values().get(0));
        values = (double[]) vector.get("values");
        assertThat(values.length, equalTo(3));
        assertThat(values[0], equalTo(0.22314355131420976));
        assertThat(values[1], equalTo(0.0));
        assertThat(values[2], equalTo(0.9162907318741551));
        indices = (int[]) vector.get("indices");
        assertThat(indices.length, equalTo(3));
        assertThat(indices[0], equalTo(2));
        assertThat(indices[1], equalTo(3));
        assertThat(indices[2], equalTo(4));
    }

    @SuppressWarnings("unchecked")
    public void testSparseVectorWithTFSomeEmpty() throws IOException, ExecutionException, InterruptedException {
        createIndexWithTermVectors();
        indexRandom(true,
                client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick"),
                client().prepareIndex().setId("2").setIndex("index").setType("type").setSource("text", ""));
        ensureGreen("index");
        refresh();
        XContentBuilder source = jsonBuilder();
        source.startObject()
                .startArray("features")
                .startObject()
                .field("field", "text")
                .field("tokens", "given")
                .field("terms", new String[]{"fox", "lame", "quick", "the", "zonk"})
                .field("number", "tf")
                .field("type", "string")
                .endObject()
                .endArray()
                .field("sparse", true)
                .endObject();
        PrepareSpecResponse specResponse = client().execute(PrepareSpecAction.INSTANCE, new PrepareSpecRequest(source.string())).get();
        Map<String, Object> params = new HashMap<>();
        params.put("spec", specResponse.getSpecAsMap());
        SearchResponse searchResponse = client().prepareSearch("index").addSort("_uid", SortOrder.ASC).addScriptField("vector", new
                Script("doc_to_vector", ScriptService.ScriptType.INLINE, "native", params)).get();
        assertSearchResponse(searchResponse);

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
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

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        vector = (Map<String, Object>) (searchResponse.getHits().getAt(1).field("vector").values().get(0));
        values = (double[]) vector.get("values");
        assertThat(values.length, equalTo(0));
        indices = (int[]) vector.get("indices");
        assertThat(indices.length, equalTo(0));
    }

    @SuppressWarnings("unchecked")

    public void testDenseVectorWithIDF() throws IOException, ExecutionException, InterruptedException {
        assertAcked(client().admin().indices().prepareCreate("index").setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)));
        indexRandom(true,
                client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick"),
                client().prepareIndex().setId("2").setIndex("index").setType("type").setSource("text", "the quick fox is brown"),
                client().prepareIndex().setId("3").setIndex("index").setType("type").setSource("text", "the brown fox is lame"),
                client().prepareIndex().setId("4").setIndex("index").setType("type").setSource("text", "the zonk is quick"));
        ensureGreen("index");
        refresh();
        XContentBuilder source = jsonBuilder();
        source.startObject()
                .startArray("features")
                .startObject()
                .field("field", "text")
                .field("tokens", "given")
                .field("terms", new String[]{"fox", "lame", "quick", "the", "zonk"})
                .field("number", "tf_idf")
                .field("type", "string")
                .endObject()
                .endArray()
                .field("sparse", false)
                .endObject();
        PrepareSpecResponse specResponse = client().execute(PrepareSpecAction.INSTANCE, new PrepareSpecRequest(source.string())).get();
        Map<String, Object> params = new HashMap<>();
        params.put("spec", specResponse.getSpecAsMap());
        SearchResponse searchResponse = client().prepareSearch("index").addSort("_uid", SortOrder.ASC).addScriptField("vector",
                new Script("doc_to_vector", ScriptService.ScriptType.INLINE, "native", params)).get();
        assertSearchResponse(searchResponse);

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        Map<String, Object> vector = (Map<String, Object>) (searchResponse.getHits().getAt(0).field("vector").values().get(0));
        double[] values = (double[]) vector.get("values");
        assertThat(values.length, equalTo(5));
        assertThat(values[0], equalTo(0.22314355131420976));
        assertThat(values[1], equalTo(0.0));
        assertThat(values[2], equalTo(0.44628710262841953));
        assertThat(values[3], equalTo(0.0));
        assertThat(values[4], equalTo(0.0));


        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        vector = (Map<String, Object>) (searchResponse.getHits().getAt(1).field("vector").values().get(0));
        values = (double[]) vector.get("values");
        assertThat(values.length, equalTo(5));
        assertThat(values[0], equalTo(0.22314355131420976));
        assertThat(values[1], equalTo(0.0));
        assertThat(values[2], equalTo(0.22314355131420976));
        assertThat(values[3], equalTo(0.0));
        assertThat(values[4], equalTo(0.0));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
        vector = (Map<String, Object>) (searchResponse.getHits().getAt(2).field("vector").values().get(0));
        values = (double[]) vector.get("values");
        assertThat(values.length, equalTo(5));
        assertThat(values[0], equalTo(0.22314355131420976));
        assertThat(values[1], equalTo(0.9162907318741551));
        assertThat(values[2], equalTo(0.0));
        assertThat(values[3], equalTo(0.0));
        assertThat(values[4], equalTo(0.0));


        assertThat(searchResponse.getHits().getAt(3).getId(), equalTo("4"));
        vector = (Map<String, Object>) (searchResponse.getHits().getAt(3).field("vector").values().get(0));
        values = (double[]) vector.get("values");
        assertThat(values.length, equalTo(5));
        assertThat(values[0], equalTo(0.0));
        assertThat(values[1], equalTo(0.0));
        assertThat(values[2], equalTo(0.22314355131420976));
        assertThat(values[3], equalTo(0.0));
        assertThat(values[4], equalTo(0.9162907318741551));

    }

    public PrepareSpecResponse createSpecWithGivenTerms(String number, boolean sparse) throws IOException, InterruptedException,
            ExecutionException {
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
                .field("type", "text")
                .field("term_vector", "yes")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        client().admin().indices().prepareCreate("index").addMapping("type", mapping).setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)).get();
    }

    @SuppressWarnings("unchecked")

    public void testVectorScriptWithSignificantTermsSortsTerms() throws IOException, ExecutionException, InterruptedException {

        client().admin().indices().prepareCreate("index").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1))
                .addMapping("type", getMapping()).get();
        client().prepareIndex().setId("1").setIndex("index").setType("type")
                .setSource("text", "I have to get up at 4am", "label", "negative").get();
        client().prepareIndex().setId("2").setIndex("index").setType("type")
                .setSource("text", "I need to get up at 5am", "label", "negative").get();
        client().prepareIndex().setId("3").setIndex("index").setType("type")
                .setSource("text", "I have to get up at 6am already", "label", "negative").get();
        client().prepareIndex().setId("4").setIndex("index").setType("type")
                .setSource("text", "I need to get up at 7am", "label", "negative").get();
        client().prepareIndex().setId("5").setIndex("index").setType("type")
                .setSource("text", "I got up at 8am", "label", "negative").get();
        client().prepareIndex().setId("6").setIndex("index").setType("type")
                .setSource("text", "I could sleep until 9am", "label", "positive").get();
        client().prepareIndex().setId("7").setIndex("index").setType("type")
                .setSource("text", "I only got up at 10am", "label", "positive").get();
        client().prepareIndex().setId("8").setIndex("index").setType("type")
                .setSource("text", "I slept until 11am", "label", "positive").get();
        client().prepareIndex().setId("9").setIndex("index").setType("type")
                .setSource("text", "I dragged myself out of bed at 12am", "label", "negative").get();
        client().prepareIndex().setId("10").setIndex("index").setType("type")
                .setSource("text", "Damn! I missed the alarm clock and got up at 1pm. Hope Clinton does not notice...", "label", "negative")
                .get();
        client().prepareIndex().setId("11").setIndex("index").setType("type")
                .setSource("text", "I fell asleep at 8pm already", "label", "positive").get();
        client().prepareIndex().setId("12").setIndex("index").setType("type")
                .setSource("text", "I fell asleep at 9pm already", "label", "positive").get();
        client().prepareIndex().setId("13").setIndex("index").setType("type")
                .setSource("text", "I fell asleep at 10pm already", "label", "positive").get();

        ensureGreen("index");
        refresh();

        PrepareSpecResponse specResponse = client().execute(PrepareSpecAction.INSTANCE, new PrepareSpecRequest
                (getTextFieldRequestSourceWithSignificnatTerms().string())).get();
        ArrayList<Map<String, Object>> features = (ArrayList<Map<String, Object>>) specResponse.getSpecAsMap().get("features");
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

    @SuppressWarnings("unchecked")

    public void testVectorScriptWithGivenTermsSortsTerms() throws IOException, ExecutionException, InterruptedException {

        PrepareSpecResponse specResponse = client().execute(PrepareSpecAction.INSTANCE, new PrepareSpecRequest
                (getTextFieldRequestSourceWithGivenTerms().string())).get();

        ArrayList<Map<String, Object>> features = (ArrayList<Map<String, Object>>) specResponse.getSpecAsMap().get("features");
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

    private XContentBuilder getMapping() throws IOException {
        XContentBuilder mapping = jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("type");
            {
                mapping.startObject("properties");
                {
                    mapping.startObject("text");
                    {
                        mapping.field("type", "text");
                        mapping.field("fielddata", true);
                    }
                    mapping.endObject();
                    mapping.startObject("label");
                    {
                        mapping.field("type", "keyword");
                    }
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        return mapping;
    }
}
