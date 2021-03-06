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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;


/**
 *
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, transportClientRatio = 0)
public class AllTermsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TokenPlugin.class);
    }

    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(TokenPlugin.class);
    }

    public void testSimpleTestOneDoc() throws Exception {
        indexDocs();
        refresh();
        AllTermsResponse response = new AllTermsRequestBuilder(client()).index("test").field("field").size(10).execute().actionGet(1000000);
        String[] expected = {"always", "be", "careful", "don't", "ever", "forget"};
        assertArrayEquals(response.allTerms.toArray(new String[2]), expected);
    }

    private void indexDocs() {
        client().prepareIndex("test", "type", "1").setSource("field", "don't  be").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "ever always forget  be").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field", "careful careful").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("field", "ever always careful careful don't be forget be").execute()
                .actionGet();
    }

    public void testSimpleTestOneDocWithFrom() throws Exception {
        indexDocs();
        refresh();
        AllTermsResponse response = new AllTermsRequestBuilder(client()).index("test").field("field").size(10).from("careful").execute()
                .actionGet(10000);
        String[] expected = {"don't", "ever", "forget"};
        assertArrayEquals(response.allTerms.toArray(new String[3]), expected);

        response = new AllTermsRequestBuilder(client()).index("test").field("field").size(10).from("ces").execute().actionGet(10000);
        String[] expected2 = {"don't", "ever", "forget"};
        assertArrayEquals(response.allTerms.toArray(new String[3]), expected2);
    }

    public void testSimpleTestOneDocWithFromAndMinDocFreq() throws Exception {
        createIndex();
        indexDocs();
        refresh();
        AllTermsResponse response = new AllTermsRequestBuilder(client()).index("test").field("field").size(10).from(" be").minDocFreq(3)
                .execute().actionGet(10000);
        String[] expected = {"be"};
        assertArrayEquals(response.allTerms.toArray(new String[1]), expected);

        response = new AllTermsRequestBuilder(client()).index("test").field("field").size(10).minDocFreq(3).from("arg").execute()
                .actionGet(10000);
        String[] expected2 = {"be"};
        assertArrayEquals(response.allTerms.toArray(new String[1]), expected2);
    }

    private void createIndex() {
        client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 1)).get();
        ensureYellow("test");
    }
}
