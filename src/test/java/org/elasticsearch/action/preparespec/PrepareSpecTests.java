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

import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class PrepareSpecTests extends ESTestCase {

    public void testParseFieldSpecRequestsWithSignificantTemrs() throws IOException {
        MappingMetaData mappingMetaData = getMappingMetaData();
        XContentBuilder source = getTextFieldRequestSourceWithSignificnatTerms();
        List<FieldSpecRequest> fieldSpecRequests = TransportPrepareSpecAction.parseFieldSpecRequests(source.string(), mappingMetaData);
        assertThat(fieldSpecRequests.size(), equalTo(1));
    }

    public void testParseFieldSpecRequestsWithAllTerms() throws IOException {
        MappingMetaData mappingMetaData = getMappingMetaData();
        XContentBuilder source = getTextFieldRequestSourceWithAllTerms();
        List<FieldSpecRequest> fieldSpecRequests = TransportPrepareSpecAction.parseFieldSpecRequests(source.string(), mappingMetaData);
        assertThat(fieldSpecRequests.size(), equalTo(1));
    }

    private MappingMetaData getMappingMetaData() throws IOException {
        XContentBuilder mapping = jsonBuilder();
        mapping.startObject();
        mapping.startObject("type");
        mapping.startObject("properties");
        mapping.startObject("text");
        mapping.field("type", "string");
        mapping.endObject();
        mapping.endObject();
        mapping.endObject();
        mapping.endObject();

        XContentParser parser = XContentFactory.xContent(mapping.bytes()).createParser(mapping.bytes());
        return new MappingMetaData("type", parser.mapOrdered());
    }

    protected static XContentBuilder getTextFieldRequestSourceWithSignificnatTerms() throws IOException {
        XContentBuilder source = jsonBuilder();
        XContentBuilder request = jsonBuilder();

        request.startObject()
                .startObject("aggregations")
                .startObject("classes")
                .startObject("terms")
                .field("field", "label")
                .endObject()
                .startObject("aggregations")
                .startObject("tokens")
                .startObject("significant_terms")
                .field("field", "text")
                .field("min_doc_count", 0)
                .field("shard_min_doc_count", 0)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        source.startObject()
                .startObject("text")
                .field("tokens", "significant_terms")
                .field("request", request.string())
                .field("index", "index")
                .field("number", "tf")
                .endObject().endObject();
        return source;
    }

    protected static XContentBuilder getTextFieldRequestSourceWithAllTerms() throws IOException {
        XContentBuilder source = jsonBuilder();
        source.startObject()
                .startObject("text")
                .field("tokens", "all_terms")
                .field("index", "index")
                .field("min_doc_freq", 2)
                .field("number", "tf")
                .endObject().endObject();
        return source;
    }
}
