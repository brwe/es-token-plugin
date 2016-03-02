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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class TransportPrepareSpecAction extends HandledTransportAction<PrepareSpecRequest, PrepareSpecResponse> {

    private final ClusterService clusterService;
    private Client client;

    @Inject
    public TransportPrepareSpecAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                      ClusterService clusterService, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver, Client client) {
        super(settings, PrepareSpecAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, PrepareSpecRequest.class);
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void doExecute(final PrepareSpecRequest request, final ActionListener<PrepareSpecResponse> listener) {
        ClusterState clusterState = clusterService.state();
        IndexMetaData indexMetaData = clusterState.getMetaData().index(request.index());
        assert (indexMetaData != null);
        MappingMetaData typeMapping = indexMetaData.getMappings().get(request.type());
        assert typeMapping != null;
        List<FieldSpecRequest> fieldSpecRequests = null;
        try {
            fieldSpecRequests = parseFieldSpecRequests(request.source(), typeMapping);
        } catch (IOException e) {
            listener.onFailure(e);
        }

        final FieldSpecActionListener fieldSpecActionListener = new FieldSpecActionListener(fieldSpecRequests.size(), listener, client);
        for (final FieldSpecRequest fieldSpecRequest : fieldSpecRequests) {
            fieldSpecRequest.process(fieldSpecActionListener, client);

        }
    }


    static List<FieldSpecRequest> parseFieldSpecRequests(String source, MappingMetaData typeMapping) throws IOException {
        List<FieldSpecRequest> fieldSpecRequests = new ArrayList<>();
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(source);
        Map<String, Object> parsedSource = parser.mapOrdered();
        for (Map.Entry<String, Object> field : parsedSource.entrySet()) {
            String type = (String) ((Map<String, Object>) ((Map<String, Object>) typeMapping.getSourceAsMap().get("properties")).get(field.getKey())).get("type");
            if (type.equals("string")) {
                fieldSpecRequests.add(new StringFieldSpecRequest((Map<String, Object>) field.getValue(), field.getKey()));
            }
        }
        return fieldSpecRequests;
    }

    static class FieldSpecActionListener implements ActionListener<FieldSpec> {

        private int numResponses;
        private ActionListener<PrepareSpecResponse> listener;
        private Client client;
        private int currentResponses;

        List<FieldSpec> fieldSpecs = new ArrayList<>();

        public FieldSpecActionListener(int numResponses, ActionListener<PrepareSpecResponse> listener, Client client) {

            this.numResponses = numResponses;
            this.listener = listener;
            this.client = client;
        }

        @Override
        public void onResponse(FieldSpec fieldSpec) {
            fieldSpecs.add(fieldSpec);
            currentResponses++;
            if (currentResponses == numResponses) {
                try {
                    client.prepareIndex("pmml", "spec").setSource(createSpecSource(fieldSpecs)).execute(new ActionListener<IndexResponse>() {
                        @Override
                        public void onResponse(IndexResponse indexResponse) {
                            listener.onResponse(new PrepareSpecResponse(indexResponse.getIndex(), indexResponse.getType(), indexResponse.getId()));
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            listener.onFailure(throwable);
                        }
                    });
                } catch (IOException e) {
                    listener.onFailure(e);
                }

            }

        }

        private XContentBuilder createSpecSource(List<FieldSpec> fieldSpecs) throws IOException {
            XContentBuilder sourceBuilder = jsonBuilder();
            sourceBuilder.startObject();
            for (FieldSpec fieldSpec : fieldSpecs) {
                fieldSpec.toXContent(sourceBuilder, ToXContent.EMPTY_PARAMS);
            }
            sourceBuilder.endObject();
            return sourceBuilder;
        }

        @Override
        public void onFailure(Throwable throwable) {
            currentResponses = numResponses;
            listener.onFailure(throwable);
        }
    }

    public static class StringFieldSpecRequest implements FieldSpecRequest {

        private String field;

        public enum TokenGenerateMethod {
            GIVEN,
            SIGNIFICANT_TERMS,
            ALL_TERMS;

            public String toString() {
                switch (this.ordinal()) {
                    case 0:
                        return "GIVEN";
                    case 1:
                        return "SIGNIFICANT_TERMS";
                    case 2:
                        return "ALL_TERMS";
                }
                throw new IllegalStateException("There is no toString() for ordinal " + this.ordinal() + " - someone forgot to implement toString().");
            }
        }

        public StringFieldSpecRequest(Map<String, Object> parameters, String field) {
            this.field = field;
            if (parameters.get("tokens").equals("significant_terms")) {
                parameters.remove("tokens");
                searchRequest = (String) parameters.remove("request");
                index = (String) parameters.remove("index");
                number = (String) parameters.remove("number");
            }
        }

        String searchRequest;
        String index;
        String number;

        @Override
        public void process(final TransportPrepareSpecAction.FieldSpecActionListener fieldSpecActionListener, Client client) {
            client.prepareSearch(this.index).setSource(this.searchRequest).execute(new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    //TODO: here get the terms from significant terms etc...

                    fieldSpecActionListener.onResponse(new StringFieldSpec(searchResponse, number, field));
                }

                @Override
                public void onFailure(Throwable throwable) {
                    fieldSpecActionListener.onFailure(throwable);
                }
            });
        }

        private class StringFieldSpec implements FieldSpec {
            String[] terms;
            String field;
            String number;
            public StringFieldSpec(SearchResponse searchResponse, String number, String field) {
                super();
                this.number = number;
                this.field = field;
                terms = extractTerms(searchResponse);
            }

            private String[] extractTerms(SearchResponse searchResponse) {
                Aggregations agg = searchResponse.getAggregations();
                assert (agg.asList().size() == 1);
                return extractTerms(agg.asList().get(0));

            }

            private String[] extractTerms(Aggregation aggregation) {
                List<String[]> terms = new ArrayList<>();
                if (aggregation instanceof MultiBucketsAggregation) {
                    for (MultiBucketsAggregation.Bucket bucket : ((MultiBucketsAggregation)(aggregation)).getBuckets()) {
                        if (bucket.getAggregations().asList().size() != 0) {
                            for (Aggregation agg : bucket.getAggregations().asList()) {
                                terms.add(extractTerms(agg));
                            }
                        } else {
                            terms.add(new String[]{bucket.getKeyAsString()});
                        }
                    }
                } else {
                    throw new IllegalStateException("cannot deal with non bucket aggs");
                }
                return combineStringArrays(terms);
            }

            private String[] combineStringArrays(List<String[]> terms) {
                int size = 0;
                for (String[] termsArray : terms) {
                    size += termsArray.length;
                }
                String[] finalTerms = new String[size];
                int curIndex = 0;
                for (String[] termsArray : terms) {
                    for (String term : termsArray) {
                        finalTerms[curIndex] = term;
                        curIndex++;
                    }
                }
                return  finalTerms;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
                xContentBuilder.startObject(field);
                xContentBuilder.field("number", number);
                xContentBuilder.field("terms", terms);
                xContentBuilder.endObject();
                return xContentBuilder;
            }
        }
    }
}
