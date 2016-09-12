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
import org.elasticsearch.action.termvectors.TermVectorsRequestBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchExtBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class TermVectorsFetchBuilder extends SearchExtBuilder {
    private final TermVectorsRequest request;

    public TermVectorsFetchBuilder(TermVectorsRequestBuilder request) {
        // index, type and id are not used - set them to spaces.
        this.request = request.setIndex("").setType("").setId("").request();
    }

    public TermVectorsFetchBuilder(TermVectorsRequest request) {
        this.request = request;
    }

    public TermVectorsFetchBuilder(StreamInput in) throws IOException {
        this.request = TermVectorsRequest.readTermVectorsRequest(in);
    }

    public TermVectorsRequest getRequest() {
        return request;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TermVectorsFetchBuilder that = (TermVectorsFetchBuilder) o;
        return Objects.equals(request, that.request);
    }

    @Override
    public int hashCode() {
        return Objects.hash(request);
    }

    @Override
    public String getWriteableName() {
        return TermVectorsFetchSubPhase.NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        request.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.array("fields", request.selectedFields());
        builder.field("offsets", request.getFlags().contains(TermVectorsRequest.Flag.Offsets));
        builder.field("positions", request.getFlags().contains(TermVectorsRequest.Flag.Positions));
        builder.field("payloads", request.getFlags().contains(TermVectorsRequest.Flag.Payloads));
        builder.field("term_statistics", request.termStatistics());
        builder.field("field_statistics", request.fieldStatistics());
        if (request.perFieldAnalyzer() != null && request.perFieldAnalyzer().isEmpty() == false) {
            builder.startObject("per_field_analyzer");
            for (Map.Entry<String, String> field : request.perFieldAnalyzer().entrySet()) {
                builder.field(field.getKey(), field.getValue());
            }
            builder.endObject();
        }
        if (request.filterSettings() != null) {
            builder.startObject("filter");
            builder.field("max_num_terms", request.filterSettings().maxNumTerms);
            builder.field("min_term_freq", request.filterSettings().minTermFreq);
            builder.field("max_term_freq", request.filterSettings().maxTermFreq);
            builder.field("min_doc_freq", request.filterSettings().maxDocFreq);
            builder.field("max_doc_freq", request.filterSettings().maxDocFreq);
            builder.field("min_word_length", request.filterSettings().minWordLength);
            builder.field("max_word_length", request.filterSettings().maxWordLength);
            builder.endObject();
        }
        return builder;
    }
}
