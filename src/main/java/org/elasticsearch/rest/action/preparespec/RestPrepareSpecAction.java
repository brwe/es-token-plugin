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

package org.elasticsearch.rest.action.preparespec;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.preparespec.PrepareSpecAction;
import org.elasticsearch.action.preparespec.PrepareSpecRequest;
import org.elasticsearch.action.preparespec.PrepareSpecResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
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
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestPrepareSpecAction extends BaseRestHandler {

    @Inject
    public RestPrepareSpecAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, "/_prepare_spec", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String id = request.param("id");
        final PrepareSpecRequest prepareSpecRequest = new PrepareSpecRequest();
        if (request.content() == null) {
            throw new ElasticsearchException("prepare spec request must have a body");
        }
        prepareSpecRequest.source(new String(BytesReference.toBytes(request.content()), Charset.defaultCharset()));
        if (id != null) {
            prepareSpecRequest.id(id);
        }
        return channel -> {
            client.execute(PrepareSpecAction.INSTANCE, prepareSpecRequest, new RestBuilderListener<PrepareSpecResponse>(channel) {
                @Override
                public RestResponse buildResponse(PrepareSpecResponse response, XContentBuilder builder) throws Exception {
                    builder.startObject();
                    response.toXContent(builder, request);
                    builder.endObject();
                    return new BytesRestResponse(OK, builder);
                }
            });
        };

    }
}
