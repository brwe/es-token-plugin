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

package org.elasticsearch.action.allterms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class TransportAllTermsAction extends HandledTransportAction<AllTermsRequest, AllTermsResponse> {

    private final ClusterService clusterService;

    private final TransportAllTermsShardAction shardAction;

    @Inject
    public TransportAllTermsAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                   ClusterService clusterService, TransportAllTermsShardAction shardAction, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, AllTermsAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                AllTermsRequest::new);
        this.clusterService = clusterService;
        this.shardAction = shardAction;
    }

    @Override
    protected void doExecute(final AllTermsRequest request, final ActionListener<AllTermsResponse> listener) {
        ClusterState clusterState = clusterService.state();

        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        final GroupShardsIterator groupShardsIterator = clusterService.operationRouting().searchShards(clusterState, request.indices(),
                null, null);
        final AtomicArray<AllTermsSingleShardResponse> shardResponses = new AtomicArray<>(groupShardsIterator.size());
        final AtomicInteger shardCounter = new AtomicInteger(shardResponses.length());
        for (final ShardIterator shardIterator : groupShardsIterator) {
            final AllTermsShardRequest shardRequest = new AllTermsShardRequest(request, request.indices()[0], shardIterator.shardId().id(),
                    request.field(), request.size(), request.from(), request.minDocFreq());
            shardAction.execute(shardRequest, new ActionListener<AllTermsSingleShardResponse>() {
                @Override
                public void onResponse(AllTermsSingleShardResponse response) {
                    shardResponses.set(shardIterator.shardId().id(), response);
                    if (shardCounter.decrementAndGet() == 0) {
                        finish();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (shardCounter.decrementAndGet() == 0) {
                        finish();
                    }
                }

                public void finish() {
                    AllTermsResponse response = new AllTermsResponse(shardResponses.toArray(
                            new AllTermsSingleShardResponse[shardResponses.length()]), request.size());
                    listener.onResponse(response);
                }
            });
        }

    }

}
