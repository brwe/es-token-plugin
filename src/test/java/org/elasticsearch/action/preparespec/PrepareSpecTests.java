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
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ParseFieldRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsParser;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicParser;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsParser;
import org.elasticsearch.search.suggest.Suggesters;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class PrepareSpecTests extends ESTestCase {
    private IndicesQueriesRegistry queryRegistry = new IndicesQueriesRegistry();

    private ParseFieldRegistry<Aggregator.Parser> aggregationParserRegistry = new ParseFieldRegistry<>("aggregation");
    private AggregatorParsers aggParsers = new AggregatorParsers(aggregationParserRegistry,
            new ParseFieldRegistry<>("pipline_aggregation"));
    private Suggesters suggesters = new Suggesters(Collections.emptyMap());
    private ParseFieldMatcher parseFieldMatcher = new ParseFieldMatcher(Settings.EMPTY);
    private ParseFieldRegistry<SignificanceHeuristicParser> significanceHeuristicParserRegistry = new ParseFieldRegistry<>(
            "significance_heuristic");

    @Override
    public void setUp() throws Exception {
        super.setUp();
        aggregationParserRegistry.register(new TermsParser(), TermsAggregationBuilder.NAME);
        aggregationParserRegistry.register(new SignificantTermsParser(significanceHeuristicParserRegistry, queryRegistry),
                SignificantTermsAggregationBuilder.NAME);
    }

    public void testParseFieldSpecRequestsWithSignificantTemrs() throws IOException {
        XContentBuilder source = getTextFieldRequestSourceWithSignificnatTerms();
        Tuple<Boolean,List<FieldSpecRequest>> fieldSpecRequests = TransportPrepareSpecAction.parseFieldSpecRequests(
                queryRegistry, aggParsers, suggesters, parseFieldMatcher, source.string());
        assertThat(fieldSpecRequests.v2().size(), equalTo(1));
    }

    public void testParseFieldSpecRequestsWithAllTerms() throws IOException {
        XContentBuilder source = getTextFieldRequestSourceWithAllTerms();
        Tuple<Boolean,List<FieldSpecRequest>> fieldSpecRequests = TransportPrepareSpecAction.parseFieldSpecRequests(
                queryRegistry, aggParsers, suggesters, parseFieldMatcher, source.string());
        assertThat(fieldSpecRequests.v2().size(), equalTo(1));
    }

    public void testParseFieldSpecRequestsWithGivenTerms() throws IOException {
        XContentBuilder source = getTextFieldRequestSourceWithGivenTerms();
        Tuple<Boolean,List<FieldSpecRequest>> fieldSpecRequests = TransportPrepareSpecAction.parseFieldSpecRequests(
                queryRegistry, aggParsers, suggesters, parseFieldMatcher, source.string());
        assertThat(fieldSpecRequests.v2().size(), equalTo(1));
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

    public static XContentBuilder getTextFieldRequestSourceWithSignificnatTerms() throws IOException {
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
                .startArray("features")
                .startObject()
                .field("type", "string")
                .field("field", "text")
                .field("tokens", "significant_terms")
                .field("request", request.string())
                .field("index", "index")
                .field("number", "tf")
                .endObject()
                .endArray()
                .field("sparse", false)
                .endObject();
        return source;
    }

    public static XContentBuilder getTextFieldRequestSourceWithAllTerms() throws IOException {
        XContentBuilder source = jsonBuilder();
        source.startObject()
                .startArray("features")
                .startObject()
                .field("field", "text")
                .field("tokens", "all_terms")
                .field("index", "index")
                .field("min_doc_freq", 2)
                .field("number", "tf")
                .field("type", "string")
                .endObject()
                .endArray()
                .field("sparse", false)
                .endObject();
        return source;
    }

    protected static XContentBuilder getTextFieldRequestSourceWithGivenTerms() throws IOException {
        XContentBuilder source = jsonBuilder();
        source.startObject()
                .startArray("features")
                .startObject()
                .field("field", "text")
                .field("tokens", "given")
                .field("terms", new String[]{"a", "b", "c"})
                .field("number", "tf")
                .field("type", "string")
                .endObject()
                .endArray()
                .field("sparse", false)
                .endObject();
        return source;
    }
}
