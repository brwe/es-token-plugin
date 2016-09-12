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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchExtBuilder;

import java.io.IOException;

/**
 * A request to analyze a text associated with a specific index. Allow to provide
 * the actual analyzer name to perform the analysis with.
 */
public class AnalyzedTextFetchBuilder extends SearchExtBuilder {

    private String analyzer;

    private String tokenizer;

    private String[] tokenFilters = Strings.EMPTY_ARRAY;

    private String[] charFilters = Strings.EMPTY_ARRAY;

    private String field;

    public AnalyzedTextFetchBuilder() {
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    public AnalyzedTextFetchBuilder(StreamInput in) throws IOException {
        analyzer = in.readOptionalString();
        tokenizer = in.readOptionalString();
        tokenFilters = in.readStringArray();
        charFilters = in.readStringArray();
        field = in.readOptionalString();
    }


    public AnalyzedTextFetchBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public String analyzer() {
        return this.analyzer;
    }

    public AnalyzedTextFetchBuilder tokenizer(String tokenizer) {
        this.tokenizer = tokenizer;
        return this;
    }

    public String tokenizer() {
        return this.tokenizer;
    }

    public AnalyzedTextFetchBuilder tokenFilters(String... tokenFilters) {
        this.tokenFilters = tokenFilters;
        return this;
    }

    public String[] tokenFilters() {
        return this.tokenFilters;
    }

    public AnalyzedTextFetchBuilder charFilters(String... charFilters) {
        this.charFilters = charFilters;
        return this;
    }

    public String[] charFilters() {
        return this.charFilters;
    }

    public AnalyzedTextFetchBuilder field(String field) {
        this.field = field;
        return this;
    }

    public String field() {
        return this.field;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(analyzer);
        out.writeOptionalString(tokenizer);
        out.writeStringArray(tokenFilters);
        out.writeStringArray(charFilters);
        out.writeOptionalString(field);
    }

    @Override
    public String getWriteableName() {
        return AnalyzedTextFetchSubPhase.NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }
        if (tokenizer != null) {
            builder.field("tokenizer", tokenizer);
        }
        if (tokenFilters != null && tokenFilters.length > 0) {
            builder.array("filters", tokenFilters);
        }
        if (charFilters != null && charFilters.length > 0) {
            builder.array("char_filters", charFilters);
        }
        if (field != null) {
            builder.array("field", field);
        }

        return null;
    }
}
