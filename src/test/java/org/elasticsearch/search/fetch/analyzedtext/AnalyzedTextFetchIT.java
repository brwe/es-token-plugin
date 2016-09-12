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

package org.elasticsearch.search.fetch.analyzedtext;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;


public class AnalyzedTextFetchIT extends ESIntegTestCase {

    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(TokenPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TokenPlugin.class);
    }

    public void testSimpleFetchAnalyzedText() throws IOException {

        client().index(
                indexRequest("test").type("type").id("1")
                        .source(jsonBuilder().startObject().field("test", "I am sam i am").endObject())).actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        ensureGreen();

        SearchSourceBuilder searchSource = SearchSourceBuilder.searchSource().ext(
                Collections.singletonList(new AnalyzedTextFetchBuilder().field("test")));
        SearchResponse response = client().prepareSearch().setSource(searchSource).get();
        assertSearchResponse(response);
        logger.info(response.toString());
        SearchHit hit = response.getHits().getAt(0);
        // get the fields from the response
        SearchHitField fields = hit.field(AnalyzedTextFetchSubPhase.NAME);
        List<String> termVectors = fields.getValue();
        assertArrayEquals(termVectors.toArray(new String[termVectors.size()]), new String[]{"i", "am", "sam", "i", "am"});
        logger.info("{}", termVectors);
    }

}
