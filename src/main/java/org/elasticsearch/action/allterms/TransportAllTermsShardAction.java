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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransportAllTermsShardAction extends TransportShardSingleOperationAction<AllTermsShardRequest, AllTermsSingleShardResponse> {

    private final IndicesService indicesService;

    private static final String ACTION_NAME = AllTermsAction.NAME + "[s]";


    @Inject
    public TransportAllTermsShardAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                        IndicesService indicesService, ThreadPool threadPool, ActionFilters actionFilters) {
        super(settings, ACTION_NAME, threadPool, clusterService, transportService, actionFilters);
        this.indicesService = indicesService;
    }

    @Override
    protected boolean isSubAction() {
        return true;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GET;
    }

    @Override
    protected AllTermsShardRequest newRequest() {
        return new AllTermsShardRequest();
    }

    @Override
    protected AllTermsSingleShardResponse newResponse() {
        return new AllTermsSingleShardResponse(null);
    }

    @Override
    protected boolean resolveIndex() {
        return false;
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        return clusterService.operationRouting()
                .getShards(state, request.concreteIndex(), request.request().shardId(), request.request().preference());
    }

    @Override
    protected AllTermsSingleShardResponse shardOperation(AllTermsShardRequest request, ShardId shardId) throws ElasticsearchException {
        List<String> terms = new ArrayList<>();
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(shardId.id());
        final Engine.Searcher searcher = indexShard.acquireSearcher("all_terms");
        IndexReader topLevelReader = searcher.reader();

        List<AtomicReaderContext> leaves = topLevelReader.leaves();

        try {
            if (leaves.size() == 0) {
                return new AllTermsSingleShardResponse(terms);
            }
            List<TermsEnum> termIters = new ArrayList<>();

            try {
                for (AtomicReaderContext reader : leaves) {
                    termIters.add(reader.reader().terms(request.field()).iterator(null));
                }
            } catch (IOException e) {
            }
            CharsRefBuilder spare = new CharsRefBuilder();
            BytesRef lastTerm = null;
            int[] exhausted = new int[termIters.size()];
            for (int i = 0; i < exhausted.length; i++) {
                exhausted[i] = 0;
            }
            try {
                //first find smallest term
                for (int i = 0; i < termIters.size(); i++) {
                    BytesRef curTerm = null;
                    if (request.from() != null) {
                        TermsEnum.SeekStatus seekStatus = termIters.get(i).seekCeil(new BytesRef(request.from()));
                        if (seekStatus.equals(TermsEnum.SeekStatus.END) == false) {
                            curTerm = termIters.get(i).term();
                        }
                    } else {
                        curTerm = termIters.get(i).next();
                    }

                    if (lastTerm == null) {
                        lastTerm = curTerm;
                        if (lastTerm == null || lastTerm.length == 0) {
                            lastTerm = null;
                            exhausted[i] = 1;
                        }
                    } else {
                        if (curTerm.compareTo(lastTerm) < 0) {
                            lastTerm = curTerm;
                        }
                    }
                }
                if (lastTerm == null) {
                    return new AllTermsSingleShardResponse(terms);
                }
                if (getDocFreq(termIters, lastTerm, request.field(), exhausted) >= request.minDocFreq()) {
                    spare.copyUTF8Bytes(lastTerm);
                    terms.add(spare.toString());
                }
                BytesRef bytesRef = new BytesRef();
                bytesRef.copyBytes(lastTerm);
                lastTerm = bytesRef;

                while (terms.size() < request.size() && lastTerm != null) {
                    moveIterators(exhausted, termIters, lastTerm, shardId);
                    lastTerm = findMinimum(exhausted, termIters, shardId);

                    if (lastTerm != null) {

                        if (getDocFreq(termIters, lastTerm, request.field(), exhausted) >= request.minDocFreq()) {
                            spare.copyUTF8Bytes(lastTerm);
                            terms.add(spare.toString());
                        }
                    }
                }
            } catch (IOException e) {
            }

            logger.trace("[{}], final terms list: {}", shardId, terms);

            return new AllTermsSingleShardResponse(terms);
        } finally {
            searcher.close();
        }
    }

    private long getDocFreq(List<TermsEnum> termIters, BytesRef lastTerm, String field, int[] exhausted) {
        long docFreq = 0;
        if (logger.isTraceEnabled()) {
            CharsRefBuilder b = new CharsRefBuilder();
            b.copyUTF8Bytes(lastTerm);
            logger.trace("Compute doc freq for {}", b.toString());
        }

        for (int i = 0; i < termIters.size(); i++) {
            if (exhausted[i] == 0) {
                try {
                    if (logger.isTraceEnabled()) {
                        CharsRefBuilder b = new CharsRefBuilder();
                        b.copyUTF8Bytes(termIters.get(i).term());
                        logger.trace("Doc freq on seg {} for term {} is {}", i, b.toString(), termIters.get(i).docFreq());
                    }

                    if (termIters.get(i).term().compareTo(lastTerm) == 0) {
                        docFreq += termIters.get(i).docFreq();
                    }
                } catch (IOException e) {

                }
            }
        }
        return docFreq;
    }

    private BytesRef findMinimum(int[] exhausted, List<TermsEnum> termIters, ShardId shardId) {
        BytesRef minTerm = null;
        for (int i = 0; i < termIters.size(); i++) {
            if (exhausted[i] == 1) {
                continue;
            }
            BytesRef candidate = null;
            try {
                candidate = termIters.get(i).term();
            } catch (IOException e) {
            }
            if (minTerm == null) {
                minTerm = candidate;

            } else {
                //it is actually smaller, so we add it
                if (minTerm.compareTo(candidate) > 0) {
                    minTerm = candidate;
                    if (logger.isTraceEnabled()) {
                        CharsRefBuilder toiString = new CharsRefBuilder();
                        toiString.copyUTF8Bytes(minTerm);
                        logger.trace("{} Setting min to  {} from segment {}", shardId, toiString.toString(), i);
                    }
                }
            }

        }
        if (minTerm != null) {
            if (logger.isTraceEnabled()) {
                CharsRefBuilder toiString = new CharsRefBuilder();
                toiString.copyUTF8Bytes(minTerm);
                logger.trace("{} final min term {}", shardId, toiString.toString());
            }
            BytesRef ret = new BytesRef();
            ret.copyBytes(minTerm);
            return ret;
        }
        return null;
    }

    private void moveIterators(int[] exhausted, List<TermsEnum> termIters, BytesRef lastTerm, ShardId shardId) {

        try {
            for (int i = 0; i < termIters.size(); i++) {
                if (exhausted[i] == 1) {
                    continue;
                }
                CharsRefBuilder toiString = new CharsRefBuilder();
                toiString.copyUTF8Bytes(lastTerm);
                logger.trace("{} lastTerm is {}", shardId, toiString.toString());
                BytesRef candidate;
                if (termIters.get(i).term().compareTo(lastTerm) == 0) {
                    candidate = termIters.get(i).next();

                    logger.trace("{} Moving segment {}", shardId, i);
                } else {
                    //it must stand on one that is greater so we just get it
                    candidate = termIters.get(i).term();
                    logger.trace("{} Not moving segment {}", shardId, i);
                }
                if (candidate == null) {
                    exhausted[i] = 1;
                } else {
                    toiString = new CharsRefBuilder();
                    toiString.copyUTF8Bytes(candidate.clone());
                    logger.trace("{} Segment is now on {}", shardId, toiString.toString());
                }
            }
        } catch (IOException e) {

        }
    }
}
