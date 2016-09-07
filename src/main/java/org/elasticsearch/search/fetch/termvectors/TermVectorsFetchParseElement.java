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

import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class TermVectorsFetchParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {

        TermVectorsFetchContext fetchSubPhaseContext = context.getFetchSubPhaseContext(TermVectorsFetchSubPhase.CONTEXT_FACTORY);
        fetchSubPhaseContext.setHitExecutionNeeded(true);
        TermVectorsRequest request = new TermVectorsRequest();
        XContentBuilder newBuilder = jsonBuilder();
        newBuilder.copyCurrentStructure(parser);
        XContentParser newParser = XContentFactory.xContent(XContentType.JSON).createParser(newBuilder.string());
        TermVectorsRequest.parseRequest(request, newParser);
        fetchSubPhaseContext.setRequest(request);

    }
}
