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
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SharedMethods;
import org.elasticsearch.script.pmml.PMMLVectorScriptEngineService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class TransportPrepareSpecAction extends HandledTransportAction<PrepareSpecRequest, PrepareSpecResponse> {

    private Client client;

    @Inject
    public TransportPrepareSpecAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                      ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver, Client client) {
        super(settings, PrepareSpecAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, PrepareSpecRequest.class);
        this.client = client;
    }

    @Override
    protected void doExecute(final PrepareSpecRequest request, final ActionListener<PrepareSpecResponse> listener) {
        Tuple<Boolean, List<FieldSpecRequest>> fieldSpecRequests = null;
        try {
            fieldSpecRequests = parseFieldSpecRequests(request.source());
        } catch (IOException e) {
            listener.onFailure(e);
        }

        final FieldSpecActionListener fieldSpecActionListener = new FieldSpecActionListener(fieldSpecRequests.v2().size(), listener, client, fieldSpecRequests.v1(), request.id());
        for (final FieldSpecRequest fieldSpecRequest : fieldSpecRequests.v2()) {
            fieldSpecRequest.process(fieldSpecActionListener, client);
        }
    }

    static Tuple<Boolean, List<FieldSpecRequest>> parseFieldSpecRequests(String source) throws IOException {
        List<FieldSpecRequest> fieldSpecRequests = new ArrayList<>();
        Map<String, Object> parsedSource = SharedMethods.getSourceAsMap(source);
        if (parsedSource.get("features") == null) {
            throw new ElasticsearchException("reatures are missing in prepare spec request");
        }
        boolean sparse = getSparse(parsedSource.get("sparse"));
        ArrayList<Map<String, Object>> actualFeatures = (ArrayList<Map<String, Object>>) parsedSource.get("features");
        for (Map<String, Object> field : actualFeatures) {

            String type = (String) field.remove("type");
            if (type == null) {
                throw new ElasticsearchException("type parameter is missing in prepare spec request");
            }
            if (type.equals("string")) {
                fieldSpecRequests.add(StringFieldSpecRequestFactory.createStringFieldSpecRequest(field));
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

        final private int numResponses;
        private ActionListener<PrepareSpecResponse> listener;
        final private Client client;
        final private boolean sparse;
        private String id;
        private int currentResponses;
        final List<FieldSpec> fieldSpecs = new ArrayList<>();

        public FieldSpecActionListener(int numResponses, ActionListener<PrepareSpecResponse> listener, Client client, boolean sparse, String id) {
            this.numResponses = numResponses;
            this.listener = listener;
            this.client = client;
            this.sparse = sparse;
            this.id = id;
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
                    final int finalLength = length;
                    IndexRequestBuilder indexRequestBuilder = client.prepareIndex(ScriptService.SCRIPT_INDEX, PMMLVectorScriptEngineService.NAME).setSource(createSpecSource(fieldSpecs, sparse, length));
                    if (id != null) {
                        indexRequestBuilder.setId(id);
                    }
                    indexRequestBuilder.execute(new ActionListener<IndexResponse>() {
                        @Override
                        public void onResponse(IndexResponse indexResponse) {
                            listener.onResponse(new PrepareSpecResponse(indexResponse.getIndex(), indexResponse.getType(), indexResponse.getId(), finalLength));
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
            XContentBuilder actualSource = jsonBuilder();
            actualSource.startObject()
                    .field("script", sourceBuilder.string())
                    .endObject();
            return actualSource;
        }

        @Override
        public void onFailure(Throwable throwable) {
            currentResponses = numResponses;
            listener.onFailure(throwable);
        }
    }
}
