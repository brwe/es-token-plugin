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

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Collection;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


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
                .endObject();
        builder.source(sourceBuilder.string());
        builder.get();
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
