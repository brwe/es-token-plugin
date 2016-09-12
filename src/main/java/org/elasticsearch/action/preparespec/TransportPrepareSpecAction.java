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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.SharedMethods;
import org.elasticsearch.search.SearchExtRegistry;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.suggest.Suggesters;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class TransportPrepareSpecAction extends HandledTransportAction<PrepareSpecRequest, PrepareSpecResponse> {

    private Client client;
    private final IndicesQueriesRegistry queryRegistry;
    private final AggregatorParsers aggParsers;
    private final Suggesters suggesters;
    private final SearchExtRegistry searchExtRegistry;
    private final ParseFieldMatcher parseFieldMatcher;


    @Inject
    public TransportPrepareSpecAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                      ActionFilters actionFilters, IndicesQueriesRegistry queryRegistry,
                                      SearchRequestParsers searchRequestParsers, SearchExtRegistry searchExtRegistry,
                                      IndexNameExpressionResolver indexNameExpressionResolver, Client client) {
        super(settings, PrepareSpecAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                PrepareSpecRequest::new);
        this.client = client;
        this.queryRegistry = queryRegistry;
        this.aggParsers = searchRequestParsers.aggParsers;
        this.suggesters = searchRequestParsers.suggesters;
        this.searchExtRegistry = searchExtRegistry;
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
    }

    @Override
    protected void doExecute(final PrepareSpecRequest request, final ActionListener<PrepareSpecResponse> listener) {
        Tuple<Boolean, List<FieldSpecRequest>> fieldSpecRequests = null;
        try {
            fieldSpecRequests = parseFieldSpecRequests(queryRegistry, aggParsers, suggesters, searchExtRegistry, parseFieldMatcher,
                    request.source());
        } catch (IOException e) {
            listener.onFailure(e);
        }

        final FieldSpecActionListener fieldSpecActionListener = new FieldSpecActionListener(fieldSpecRequests.v2().size(), listener,
                fieldSpecRequests.v1());
        for (final FieldSpecRequest fieldSpecRequest : fieldSpecRequests.v2()) {
            fieldSpecRequest.process(fieldSpecActionListener, client);
        }
    }

    static Tuple<Boolean, List<FieldSpecRequest>> parseFieldSpecRequests(IndicesQueriesRegistry queryRegistry, AggregatorParsers aggParsers,
                                                                         Suggesters suggesters, SearchExtRegistry searchExtRegistry,
                                                                         ParseFieldMatcher parseFieldMatcher,
                                                                         String source) throws IOException {
        List<FieldSpecRequest> fieldSpecRequests = new ArrayList<>();
        Map<String, Object> parsedSource = SharedMethods.getSourceAsMap(source);
        if (parsedSource.get("features") == null) {
            throw new ElasticsearchException("reatures are missing in prepare spec request");
        }
        boolean sparse = getSparse(parsedSource.get("sparse"));
        @SuppressWarnings("unchecked") ArrayList<Map<String, Object>> actualFeatures =
                (ArrayList<Map<String, Object>>) parsedSource.get("features");
        for (Map<String, Object> field : actualFeatures) {

            String type = (String) field.remove("type");
            if (type == null) {
                throw new ElasticsearchException("type parameter is missing in prepare spec request");
            }
            if (type.equals("string")) {
                fieldSpecRequests.add(StringFieldSpecRequestFactory.createStringFieldSpecRequest(queryRegistry, aggParsers, suggesters,
                        parseFieldMatcher, searchExtRegistry, field));
            } else {
                throw new UnsupportedOperationException("I am working as quick as I can! But I have not done it for " + type + " yet.");
            }
        }
        return new Tuple<>(sparse, fieldSpecRequests);
    }

    public static boolean getSparse(Object sparse) {
        if (sparse == null) {
            return false;
        }
        if (sparse instanceof Boolean) {
            return (Boolean) sparse;
        }
        if (sparse instanceof String) {
            if (sparse.equals("false")) {
                return false;
            }
            if (sparse.equals("true")) {
                return true;
            }
        }
        throw new IllegalStateException("don't know what sparse: " + sparse + " means!");
    }

    public static class FieldSpecActionListener implements ActionListener<FieldSpec> {

        private final int numResponses;
        private ActionListener<PrepareSpecResponse> listener;
        private final boolean sparse;
        private int currentResponses;
        final List<FieldSpec> fieldSpecs = new ArrayList<>();

        public FieldSpecActionListener(int numResponses, ActionListener<PrepareSpecResponse> listener, boolean sparse) {
            this.numResponses = numResponses;
            this.listener = listener;
            this.sparse = sparse;
        }

        @Override
        public void onResponse(FieldSpec fieldSpec) {
            fieldSpecs.add(fieldSpec);
            currentResponses++;
            if (currentResponses == numResponses) {
                try {
                    int length = 0;
                    for (FieldSpec fS : fieldSpecs) {
                        length += fS.getLength();
                    }
                    listener.onResponse(new PrepareSpecResponse(createSpecSource(fieldSpecs, sparse, length).bytes(), length));
                } catch (IOException e) {
                    listener.onFailure(e);
                }
            }
        }

        public static XContentBuilder createSpecSource(List<FieldSpec> fieldSpecs, boolean sparse, int length) throws IOException {

            XContentBuilder sourceBuilder = jsonBuilder();
            sourceBuilder.startObject();
            sourceBuilder.field("sparse", sparse);
            sourceBuilder.startArray("features");
            for (FieldSpec fieldSpec : fieldSpecs) {
                fieldSpec.toXContent(sourceBuilder, ToXContent.EMPTY_PARAMS);
            }
            sourceBuilder.endArray();
            sourceBuilder.field("length", Integer.toString(length));
            sourceBuilder.endObject();
            return sourceBuilder;
        }

        @Override
        public void onFailure(Exception throwable) {
            currentResponses = numResponses;
            listener.onFailure(throwable);
        }
    }
}
