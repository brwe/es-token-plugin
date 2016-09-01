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

package org.elasticsearch.ml.training;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.ml.training.ModelTrainer.TrainingSession;
import org.elasticsearch.search.SearchRequestParsers;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Generic service for training models using aggregations framework.
 */
public class TrainingService extends AbstractComponent {
    private final ModelTrainers modelTrainers;
    private final Client client;
    private final ClusterService clusterService;
    private final SearchRequestParsers searchRequestParsers;
    private final ParseFieldMatcher parseFieldMatcher;

    public TrainingService(Settings settings, ClusterService clusterService, Client client, ModelTrainers modelTrainers,
                           SearchRequestParsers searchRequestParsers) {
        super(settings);
        this.clusterService = clusterService;
        this.modelTrainers = modelTrainers;
        this.client = client;
        this.searchRequestParsers = searchRequestParsers;
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
    }

    public void train(String modelType, Settings modelSettings, String index, String type, Map<String, Object> query,
                      List<ModelInputField> fields, ModelTargetField outputField, ActionListener<String> listener) {
        try {
            MetaData metaData = clusterService.state().getMetaData();
            AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(index);
            if (aliasOrIndex == null) {
                throw new IndexNotFoundException("the training index [" + index + "] not found");
            }
            if (aliasOrIndex.getIndices().size() != 1) {
                throw new IllegalArgumentException("can only train on a single index");
            }
            IndexMetaData indexMetaData = aliasOrIndex.getIndices().get(0);
            MappingMetaData mappingMetaData = indexMetaData.mapping(type);
            if (mappingMetaData == null) {
                throw new ResourceNotFoundException("the training type [" + type + "] not found");
            }

            Optional<QueryBuilder> queryBuilder = query.isEmpty() ? Optional.empty() : parseQuery(query);

            TrainingSession trainingSession =
                    modelTrainers.createTrainingSession(mappingMetaData, modelType, modelSettings, fields, outputField);


            SearchRequestBuilder searchRequestBuilder =
                    client.prepareSearch().setIndices(index).addAggregation(trainingSession.trainingRequest());
            if (queryBuilder.isPresent()) {
                searchRequestBuilder = searchRequestBuilder.setQuery(queryBuilder.get());
            }
            searchRequestBuilder.execute(new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    try {
                        listener.onResponse(trainingSession.model(response));
                    } catch (Exception ex) {
                        listener.onFailure(ex);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });

        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }

    private Optional<QueryBuilder> parseQuery(Map<String, Object> query) {
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(
                XContentFactory.contentBuilder(XContentType.JSON).map(query).string())) {
            QueryParseContext context = new QueryParseContext(searchRequestParsers.queryParsers, parser, parseFieldMatcher);
            return context.parseInnerQueryBuilder();
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot parse query [" + query + "]", e);
        }
    }
}
