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
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequestBuilder;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestBuilderListener;
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
    public RestStoreModelAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, "/_store_model", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final NodeClient client) {
        final String id;
        if (request.hasParam("id")) {
            id = request.param("id");
        } else {
            id = UUIDs.randomBase64UUID();
        }
        if (request.content() == null) {
            throw new ElasticsearchException("_store_model request must have a body");
        }
        Map<String, Object> sourceAsMap;
        try {
            sourceAsMap = SharedMethods.getSourceAsMap(new String(BytesReference.toBytes(request.content()), Charset.defaultCharset()));
        } catch (IOException e) {
            throw new ElasticsearchException("cannot store model", e);
        }

        if (sourceAsMap.get("model") == null) {
            throw new ElasticsearchException("spec is missing from _store_model request");
        }

        final String model = (String) sourceAsMap.get("model");

        storeModel(channel, client, id, model);
    }

    public void storeModel(final RestChannel channel, Client client, String id, String model) {
        PutStoredScriptRequestBuilder storedScriptRequestBuilder;
        try {
            storedScriptRequestBuilder = client.admin().cluster().preparePutStoredScript().setScriptLang(PMMLModelScriptEngineService.NAME)
                    .setSource(jsonBuilder().startObject().field("script", model).endObject().bytes());
        } catch (IOException e) {
            throw new ElasticsearchException("cannot store model", e);
        }
        if (id != null) {
            storedScriptRequestBuilder.setId(id);
        }
        storedScriptRequestBuilder.execute(new RestBuilderListener<PutStoredScriptResponse>(channel){
            @Override
            public RestResponse buildResponse(PutStoredScriptResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field("acknowledged", response.isAcknowledged());
                builder.field("id", id);
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }
}
