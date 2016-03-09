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
        String id = request.param("id");
        if (request.content() == null) {
            throw new ElasticsearchException("_store_model request must have a body");
        }
        Map<String, Object> sourceAsMap;
        try {
            sourceAsMap = SharedMethods.getSourceAsMap(new String(request.content().toBytes(), Charset.defaultCharset()));
        } catch (IOException e) {
            throw new ElasticsearchException("cannot store model", e);
        }

        if (sourceAsMap.get("spec") == null) {
            throw new ElasticsearchException("spec is missing from _store_model request");
        }
        if (sourceAsMap.get("model") == null) {
            throw new ElasticsearchException("spec is missing from _store_model request");
        }
        String finalModel = sourceAsMap.get("spec") + PMMLModelScriptEngineService.Factory.VECTOR_MODEL_DELIMITER + sourceAsMap.get("model");
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
