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

package org.elasticsearch.rest.action.updatemodelspec;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.pmml.PMMLModelScriptEngineService;
import org.elasticsearch.script.pmml.PMMLVectorScriptEngineService;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * Replaces the spec for a model with a different one. We need that when for example the fieldname in a document is different
 * from the fieldname for docs we trained on.
 */
public class RestUpdateModelSpecAction extends BaseRestHandler {

    @Inject
    public RestUpdateModelSpecAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_update_model_spec", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final String model_id = request.param("model_id");
        final String spec_id = request.param("spec_id");
        final String new_model_id = request.param("id");
        if (model_id == null) {
            throw new ElasticsearchException("_update_model_spec needs a model_id");
        }
        if (spec_id == null) {
            throw new ElasticsearchException("_update_model_spec needs a spec_id");
        }
        client.prepareMultiGet()
                .add(new MultiGetRequest.Item(ScriptService.SCRIPT_INDEX, PMMLVectorScriptEngineService.NAME, spec_id))
                .add(new MultiGetRequest.Item(ScriptService.SCRIPT_INDEX, PMMLModelScriptEngineService.NAME, model_id)).execute(new ActionListener<MultiGetResponse>() {
            @Override
            public void onResponse(MultiGetResponse getFields) {
                for (MultiGetItemResponse itemResponse : getFields.getResponses()) {
                    if (itemResponse.isFailed()) {
                        throw new ElasticsearchException("could not find " + itemResponse.getId());
                    }
                }
                MultiGetItemResponse model = getFields.getResponses()[1];
                String modelString = (String) model.getResponse().getSourceAsMap().get("script");
                String[] modelAndVector = modelString.split(PMMLModelScriptEngineService.Factory.VECTOR_MODEL_DELIMITER);
                storeModel(channel, client, new_model_id, (String) getFields.getResponses()[0].getResponse().getSource().get("script"), modelAndVector[1]);
            }

            @Override
            public void onFailure(Throwable throwable) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, throwable));
                } catch (IOException e) {
                    logger.error("could not send back failure method");
                }
            }
        });
    }

    public void storeModel(final RestChannel channel, Client client, String id, String spec, String model) {
        String finalModel = spec + PMMLModelScriptEngineService.Factory.VECTOR_MODEL_DELIMITER + model;
        IndexRequestBuilder indexRequestBuilder;
        try {
            indexRequestBuilder = client.prepareIndex(ScriptService.SCRIPT_INDEX, PMMLModelScriptEngineService.NAME).setSource(jsonBuilder().startObject().field("script", finalModel).endObject());
        } catch (IOException e) {
            throw new ElasticsearchException("cannot store model", e);
        }
        if (id != null) {
            indexRequestBuilder.setId(id);
        }
        indexRequestBuilder.execute(new RestBuilderListener<IndexResponse>(channel) {
            @Override
            public RestResponse buildResponse(IndexResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field("index", response.getIndex());
                builder.field("type", response.getType());
                builder.field("id", response.getId());
                builder.field("version", response.getVersion());
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }
}
