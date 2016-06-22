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

package org.elasticsearch.action.trainnaivebayes;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.FullPMMLIT;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.pmml.PMMLModelScriptEngineService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;


@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, transportClientRatio = 0)
public class TrainNaiveBayesIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(TokenPlugin.class);
    }

    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return pluginList(TokenPlugin.class);
    }

    @Test
    public void testNaiveBayesTraining() throws Exception {
        indexDocs();
        refresh();
        TrainNaiveBayesRequestBuilder builder = new TrainNaiveBayesRequestBuilder(client());
        XContentBuilder sourceBuilder = jsonBuilder();
        sourceBuilder.startObject()
                .field("fields", new String[]{"text", "num"})
                .field("target_field", "label")
                .field("index", "index")
                .field("type", "type")
                .field("id", "abcd")
                .endObject();
        builder.source(sourceBuilder.string());
        TrainNaiveBayesResponse response = builder.get();
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("pmml", new Script(response.getId(), ScriptService
                .ScriptType
                .INDEXED, PMMLModelScriptEngineService.NAME, new HashMap<String, Object>())).addField("_source").setSize(10000).get();
        assertSearchResponse(searchResponse);
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            String label = (String) ((Map<String, Object>) (hit.field("pmml").values().get(0))).get("class");
            assertThat(label, anyOf(equalTo("good"), equalTo("bad")));
        }

    }

    @Test
    public void testNaiveBayesTrainingInElasticsearchSameAsInR() throws Exception {
        FullPMMLIT.indexAdultData("/org/elasticsearch/script/adult.data", this);
        FullPMMLIT.indexAdultModel("/org/elasticsearch/script/naive-bayes-adult-full-r-no-missing-values.xml");
        refresh();

        SearchResponse aggResponse =client().prepareSearch("test").addAggregation(terms("class").field("class").size(Integer.MAX_VALUE)
                .shardMinDocCount(1)
                .minDocCount(1).order(Terms.Order.term(true))).get();
        assertThat(((Terms) aggResponse.getAggregations().getAsMap().get("class")).getBuckets().size(), equalTo(2));
        TrainNaiveBayesRequestBuilder builder = new TrainNaiveBayesRequestBuilder(client());
        XContentBuilder sourceBuilder = jsonBuilder();

        sourceBuilder.startObject()
                .field("fields", new String[]{"age", "workclass", "fnlwgt", "education", "education_num", "marital_status", "occupation",
                        "relationship", "race", "sex", "capital_gain", "capital_loss", "hours_per_week", "native_country"})
                .field("target_field", "class")
                .field("index", "test")
                .field("type", "type")
                .field("id", "abcd")
                .endObject();
        builder.source(sourceBuilder.string());
        TrainNaiveBayesResponse response = builder.get();
        SearchResponse searchResponseEsModel = client().prepareSearch("test").addScriptField("pmml", new Script(response.getId(),
                ScriptService
                .ScriptType
                .INDEXED, PMMLModelScriptEngineService.NAME, new HashMap<String, Object>())).addField("_source").setSize(10000).addSort
                ("_id", SortOrder.ASC).get();
        assertSearchResponse(searchResponseEsModel);
        SearchResponse searchResponseRModel = client().prepareSearch("test").addScriptField("pmml", new Script("1",
                ScriptService
                .ScriptType
                .INDEXED, PMMLModelScriptEngineService.NAME, new HashMap<String, Object>())).addField("_source").setSize(10000).addSort
                ("_id", SortOrder.ASC).get();
        assertSearchResponse(searchResponseRModel);

        int hitCounter = 0;
        for (SearchHit hit : searchResponseEsModel.getHits().getHits()) {
            String Rlabel = (String) ((Map<String, Object>) (hit.field("pmml").values().get(0))).get("class");
            String esLabel = (String) ((Map<String, Object>) (searchResponseEsModel.getHits().getHits()[hitCounter].field("pmml").values().get(0)))
            .get("class");
            hitCounter++;

            Map<String, Double> RProbs= (Map<String, Double>) ((Map<String, Object>) (hit.field("pmml").values().get(0))).get("probs");
            Map<String, Double> esProbs = (Map<String, Double>) ((Map<String, Object>) (searchResponseEsModel.getHits().getHits()[hitCounter].field
                    ("pmml").values().get(0))).get("probs");
            assertThat(esProbs.get(">50K"), equalTo(RProbs.get(">50K")));
            assertThat(esProbs.get("<=50K"), equalTo(RProbs.get("<=50K")));
            assertThat(esLabel, equalTo(Rlabel));
        }

    }


    private void indexDocs() {
        client().admin().indices().prepareCreate("index").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1))
                .get();
        client().prepareIndex("index", "type", "1").setSource("text", "I hate json", "label", "bad", "num", 1).execute().actionGet();
        client().prepareIndex("index", "type", "2").setSource("text", "json sucks", "label", "bad", "num", 2).execute().actionGet();
        client().prepareIndex("index", "type", "3").setSource("text", "json is much worse than xml", "label", "bad", "num", 3).execute()
                .actionGet();
        client().prepareIndex("index", "type", "4").setSource("text", "xml is lovely", "label", "good", "num", 4).execute().actionGet();
        client().prepareIndex("index", "type", "5").setSource("text", "everyone loves xml", "label", "good", "num", 5).execute()
                .actionGet();
        client().prepareIndex("index", "type", "6").setSource("text", "seriously, xml is sooo much better than json", "label", "good",
                "num", 6)
                .execute().actionGet();
        client().prepareIndex("index", "type", "7").setSource("text", "if any of my fellow developers reads this, they will tar and " +
                        "feather me and hang my mutilated body above the entrace to amsterdam headquaters as a warning to others", "label",
                "good", "num", 7).execute().actionGet();
        client().prepareIndex("index", "type", "8").setSource("text", "obviously I am joking", "label", "good", "num", 8).execute()
                .actionGet();
    }

}
