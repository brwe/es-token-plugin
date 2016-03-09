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

package org.elasticsearch.rest.action.storemodel;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SharedMethods;
import org.elasticsearch.script.pmml.PMMLModelScriptEngineService;
import org.elasticsearch.script.pmml.PMMLVectorScriptEngineService;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestStoreModelAction extends BaseRestHandler {

    @Inject
    public RestStoreModelAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_store_model", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final String id = request.param("id");
        final String spec_id = request.param("spec_id");
        if (request.content() == null) {
            throw new ElasticsearchException("_store_model request must have a body");
        }
        Map<String, Object> sourceAsMap;
        try {
            sourceAsMap = SharedMethods.getSourceAsMap(new String(request.content().toBytes(), Charset.defaultCharset()));
        } catch (IOException e) {
            throw new ElasticsearchException("cannot store model", e);
        }

        if (sourceAsMap.get("spec") == null && spec_id == null) {
            throw new ElasticsearchException("spec is missing from _store_model request and no spec_id given");
        }
        if (sourceAsMap.get("spec") != null && spec_id != null) {
            throw new ElasticsearchException("spec is given in body and spec id is given too (" + spec_id + ")- don't know which one I should use");
        }
        if (sourceAsMap.get("model") == null) {
            throw new ElasticsearchException("spec is missing from _store_model request");
        }

        final String model = (String) sourceAsMap.get("model");

        if (sourceAsMap.get("spec") == null) {
            client.prepareGet(ScriptService.SCRIPT_INDEX, PMMLVectorScriptEngineService.NAME, spec_id).execute(new ActionListener<GetResponse>() {
                @Override
                public void onResponse(GetResponse getFields) {
                    if (getFields.isExists() == false) {
                        throw new ElasticsearchException("spec_id is not valid - spec " + spec_id + " does not exist");
                    }
                    storeModel(channel, client, id, (String) getFields.getSource().get("script"), model);
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
        } else {
            String spec = (String) sourceAsMap.get("spec");
            storeModel(channel, client, id, spec, model);
        }
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
