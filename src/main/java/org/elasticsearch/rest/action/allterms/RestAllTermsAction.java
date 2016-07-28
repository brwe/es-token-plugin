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

package org.elasticsearch.rest.action.allterms;

import org.elasticsearch.action.allterms.AllTermsAction;
import org.elasticsearch.action.allterms.AllTermsRequest;
import org.elasticsearch.action.allterms.AllTermsResponse;
import org.elasticsearch.client.Client;
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

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestAllTermsAction extends BaseRestHandler {

    @Inject
    public RestAllTermsAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(GET, "/{index}/_allterms/{field}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final AllTermsRequest allTermsRequest = new AllTermsRequest();
        allTermsRequest.index(request.param("index"));
        allTermsRequest.field(request.param("field"));
        allTermsRequest.size(request.paramAsInt("size", 10));
        allTermsRequest.from(request.param("from"));
        allTermsRequest.minDocFreq(request.paramAsLong("min_doc_freq", 0));

        client.execute(AllTermsAction.INSTANCE, allTermsRequest, new RestBuilderListener<AllTermsResponse>(channel) {
            @Override
            public RestResponse buildResponse(AllTermsResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                response.toXContent(builder, request);
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }
}
