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

package org.elasticsearch.index.mapper.token;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class AnalyzedTextTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(TokenPlugin.class);
    }


    @Test
    public void testAnalyzedText() throws IOException {
        XContentBuilder mapping = jsonBuilder();
        mapping.startObject()
                .startObject("type")
                .startObject("properties")
                .startObject("text")
                .field("type", "analyzed_text")
                .field("store", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        client().admin().indices().prepareCreate("index").addMapping("type", mapping).get();
        client().prepareIndex("index", "type", "1").setSource("text", "I i am sam").get();
        refresh();
        GetResponse getResponse = client().prepareGet("index", "type", "1").setFields("text").get();
        String[] expected = {"i", "i", "am", "sam"};
        assertArrayEquals(getResponse.getField("text").getValues().toArray(new String[getResponse.getField("text").getValues().size()]), expected);
        SearchResponse searchResponse = client().prepareSearch("index").addAggregation(terms("terms").field("text")).get();
        List<Terms.Bucket> terms = ((Terms) searchResponse.getAggregations().get("terms")).getBuckets();
        for (Terms.Bucket bucket : terms) {
            if (bucket.getKey().equals("i")) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            }
        }
    }
}
