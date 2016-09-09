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

package org.elasticsearch.action.trainmodel;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequestBuilder;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ml.training.TrainingService;
import org.elasticsearch.script.pmml.PMMLModelScriptEngineService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class TransportTrainModelAction extends HandledTransportAction<TrainModelRequest, TrainModelResponse> {

    private final Client client;
    private final TrainingService trainingService;

    @Inject
    public TransportTrainModelAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                     ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver, Client client,
                                     TrainingService trainingService) {
        super(settings, TrainModelAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                TrainModelRequest::new);
        this.client = client;
        this.trainingService = trainingService;
    }

    @Override
    protected void doExecute(final TrainModelRequest request, final ActionListener<TrainModelResponse> listener) {
        trainingService.train(request.getModelType(), request.getModelSettings(),
                request.getTrainingSet().getIndex(), request.getTrainingSet().getType(),
                request.getTrainingSet().getQuery(), request.getFields(), request.getTargetField(),
                new ActionListener<String>() {
                    @Override
                    public void onResponse(String s) {
                        try {
                            storeOrReturnModel(request.getModelId(), s, listener);
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
    }

    private void storeOrReturnModel(String id, String model, ActionListener<TrainModelResponse> listener) {
        PutStoredScriptRequestBuilder storedScriptRequestBuilder;
        try {
            storedScriptRequestBuilder = client.admin().cluster().preparePutStoredScript().setScriptLang(PMMLModelScriptEngineService.NAME)
                    .setSource(jsonBuilder().startObject().field("script", model).endObject().bytes());
        } catch (IOException e) {
            throw new ElasticsearchException("cannot store model", e);
        }
        if (id == null) {
            TrainModelResponse modelResponse = new TrainModelResponse();
            modelResponse.setModel(model);
            listener.onResponse(modelResponse);
        } else {
            storedScriptRequestBuilder.setId(id);
            storedScriptRequestBuilder.execute(new ActionListener<PutStoredScriptResponse>() {
                @Override
                public void onResponse(PutStoredScriptResponse putStoredScriptResponse) {
                    TrainModelResponse modelResponse = new TrainModelResponse();
                    modelResponse.setId(id);
                    listener.onResponse(modelResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }

}
