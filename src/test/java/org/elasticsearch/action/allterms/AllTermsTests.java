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

package org.elasticsearch.action.allterms;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;


/**
 *
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, transportClientRatio = 0)
public class AllTermsTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .build();
    }

    @Test
    public void testSimpleTestOneDoc() throws Exception {
        client().prepareIndex("test", "type", "1").setSource("field", "foo bar").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "I am sam bar").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field", "blah blah").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("field", "I am blah blah foo bar sam bar").execute().actionGet();
        refresh();
        AllTermsResponse response = new AllTermsRequestBuilder(client()).index("test").field("field").size(10).execute().actionGet(1000000);
        String[] expected = {"am", "bar", "blah", "foo", "i", "sam"};
        assertArrayEquals(response.allTerms.toArray(new String[2]), expected);
    }


    @Test
    public void testSimpleTestOneDocWithFrom() throws Exception {
        client().prepareIndex("test", "type", "1").setSource("field", "foo bar").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "I am sam bar").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field", "blah blah").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("field", "I am blah blah foo bar sam bar").execute().actionGet();
        refresh();
        AllTermsResponse response = new AllTermsRequestBuilder(client()).index("test").field("field").size(10).from("blah").execute().actionGet(10000);
        String[] expected = {"blah", "foo", "i", "sam"};
        assertArrayEquals(response.allTerms.toArray(new String[4]), expected);

        response = new AllTermsRequestBuilder(client()).index("test").field("field").size(10).from("ces").execute().actionGet(10000);
        String[] expected2 = {"foo", "i", "sam"};
        assertArrayEquals(response.allTerms.toArray(new String[3]), expected2);
    }

    @Test
    @TestLogging("org.elasticsearch.action.allterms:TRACE")
    public void testSimpleTestOneDocWithFromAndMinDocFreq() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).get();
        ensureYellow("test");
        client().prepareIndex("test", "type", "1").setSource("field", "foo bar").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "I am sam bar").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field", "blah blah").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("field", "I am blah blah foo bar sam bar").execute().actionGet();
        refresh();
        AllTermsResponse response = new AllTermsRequestBuilder(client()).index("test").field("field").size(10).from("bar").minDocFreq(3).execute().actionGet(10000);
        String[] expected = {"bar"};
        assertArrayEquals(response.allTerms.toArray(new String[1]), expected);

        response = new AllTermsRequestBuilder(client()).index("test").field("field").size(10).minDocFreq(3).from("arg").execute().actionGet(10000);
        String[] expected2 = {"bar"};
        assertArrayEquals(response.allTerms.toArray(new String[1]), expected2);
    }


}
