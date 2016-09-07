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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.SharedMethods;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class AnalyzedTextFetchParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {

        AnalyzedTextFetchContext analyzedTextFetchContext = context.getFetchSubPhaseContext(AnalyzedTextFetchSubPhase.CONTEXT_FACTORY);
        analyzedTextFetchContext.setHitExecutionNeeded(true);

        XContentBuilder newBuilder = jsonBuilder();
        newBuilder.copyCurrentStructure(parser);
        Map<String, Object> requestAsMap = SharedMethods.getSourceAsMap(newBuilder.string());
        AnalyzedTextRequest request = new AnalyzedTextRequest();
        if (requestAsMap.get("analyzer") != null) {
            request.analyzer((String) requestAsMap.remove("analyzer"));
        }
        if (requestAsMap.get("token_filters") != null) {
            request.tokenFilters((String[]) requestAsMap.remove("token_filters"));
        }
        if (requestAsMap.get("tokenizer") != null) {
            request.tokenizer((String) requestAsMap.remove("tokenizer"));
        }
        if (requestAsMap.get("char_filters") != null) {
            request.charFilters((String[]) requestAsMap.remove("char_filters"));
        }
        if (requestAsMap.get("field") != null) {
            request.field((String) requestAsMap.remove("field"));
        }
        assert requestAsMap.isEmpty();
        analyzedTextFetchContext.setRequest(request);

    }

}
