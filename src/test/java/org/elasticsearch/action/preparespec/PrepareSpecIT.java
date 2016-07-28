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

package org.elasticsearch.action.preparespec;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.modelinput.VectorRangesToVector;
import org.elasticsearch.script.modelinput.VectorRangesToVectorJSON;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.action.preparespec.PrepareSpecTests.getTextFieldRequestSourceWithAllTerms;
import static org.elasticsearch.action.preparespec.PrepareSpecTests.getTextFieldRequestSourceWithGivenTerms;
import static org.elasticsearch.action.preparespec.PrepareSpecTests.getTextFieldRequestSourceWithSignificnatTerms;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;


@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, transportClientRatio = 0)
public class PrepareSpecIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(TokenPlugin.class);
    }

    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return pluginList(TokenPlugin.class);
    }

    public void testSimpleTextFieldRequestWithSignificantTerms() throws Exception {
        indexDocs();
        refresh();
        PrepareSpecResponse prepareSpecResponse = new PrepareSpecRequestBuilder(client()).source(
                getTextFieldRequestSourceWithSignificnatTerms().string()).setId("my_id").get();
        assertThat(prepareSpecResponse.getLength(), greaterThan(0));
        Map<String, Object> parsedSource = prepareSpecResponse.getSpecAsMap();
        VectorRangesToVector entries = new VectorRangesToVectorJSON(parsedSource);
        assertThat(entries.isSparse(), equalTo(false));
        assertThat(entries.getEntries().size(), equalTo(1));
    }

    public void testSimpleTextFieldRequestWithAllTerms() throws Exception {
        indexDocs();
        refresh();
        PrepareSpecResponse prepareSpecResponse = new PrepareSpecRequestBuilder(client()).source(
                getTextFieldRequestSourceWithAllTerms().string()).get();
        assertThat(prepareSpecResponse.getLength(), equalTo(6));
        Map<String, Object> parsedSource = prepareSpecResponse.getSpecAsMap();
        VectorRangesToVector entries = new VectorRangesToVectorJSON(parsedSource);
        assertThat(entries.isSparse(), equalTo(false));
        assertThat(entries.getEntries().size(), equalTo(1));
    }

    public void testSimpleTextFieldRequestWithGivenTerms() throws Exception {
        indexDocs();
        refresh();
        PrepareSpecResponse prepareSpecResponse = new PrepareSpecRequestBuilder(client()).source(
                getTextFieldRequestSourceWithGivenTerms().string()).get();
        assertThat(prepareSpecResponse.getLength(), equalTo(3));
        Map<String, Object> parsedSource = prepareSpecResponse.getSpecAsMap();
        VectorRangesToVector entries = new VectorRangesToVectorJSON(parsedSource);
        assertThat(entries.isSparse(), equalTo(false));
        assertThat(entries.getEntries().size(), equalTo(1));
    }

    private void indexDocs() throws IOException {
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
        client().admin().indices().prepareCreate("index").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1))
                .addMapping("type", mapping).get();
        client().prepareIndex("index", "type", "1").setSource("text", "I hate json", "label", "bad").execute().actionGet();
        client().prepareIndex("index", "type", "2").setSource("text", "json sucks", "label", "bad").execute().actionGet();
        client().prepareIndex("index", "type", "2").setSource("text", "json is much worse than xml", "label", "bad").execute().actionGet();
        client().prepareIndex("index", "type", "3").setSource("text", "xml is lovely", "label", "good").execute().actionGet();
        client().prepareIndex("index", "type", "4").setSource("text", "everyone loves xml", "label", "good").execute().actionGet();
        client().prepareIndex("index", "type", "3").setSource("text", "seriously, xml is sooo much better than json", "label", "good")
                .execute().actionGet();
        client().prepareIndex("index", "type", "4").setSource("text", "if any of my fellow developers reads this, they will tar and " +
                "feather me and hang my mutilated body above the entrace to amsterdam headquaters as a warning to others", "label", "good")
                .execute().actionGet();
        client().prepareIndex("index", "type", "4").setSource("text", "obviously I am joking", "label", "good").execute().actionGet();
    }

}
