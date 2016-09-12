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
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.ml.training.ModelTrainer.TrainingSession;
import org.elasticsearch.plugins.SearchPlugin.QuerySpec;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 */
public class TrainingServiceTests extends ESTestCase {

    private static final IndicesQueriesRegistry indicesQueriesRegistry = new IndicesQueriesRegistry();
    static {
        // Register a few simple queries to test parsing
        registerQuery(new QuerySpec<>(MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new, MatchAllQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(TermQueryBuilder.NAME, TermQueryBuilder::new, TermQueryBuilder::fromXContent));
    }

    private static void registerQuery(QuerySpec<?> spec) {
        indicesQueriesRegistry.register(spec.getParser(), spec.getName());
    }

    private final SearchRequestParsers searchParsers = new SearchRequestParsers(indicesQueriesRegistry, null, null, null);

    private final Settings MINIMAL_INDEX_SETTINGS = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();


    private SearchRequestBuilder mockSearchRequestBuilder(String index, AggregationBuilder aggregationBuilder) {
        return mockSearchRequestBuilder(index, aggregationBuilder, null);
    }

    private SearchRequestBuilder mockSearchRequestBuilder(String index, AggregationBuilder aggregationBuilder, QueryBuilder queryBuilder) {
        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        when(searchRequestBuilder.setIndices(index)).thenReturn(searchRequestBuilder);
        if (queryBuilder == null) {
            when(searchRequestBuilder.setQuery(any(QueryBuilder.class))).thenReturn(searchRequestBuilder);
        } else {
            when(searchRequestBuilder.setQuery(eq(queryBuilder))).thenReturn(searchRequestBuilder);
        }
        when(searchRequestBuilder.addAggregation(aggregationBuilder)).thenReturn(searchRequestBuilder);
        return searchRequestBuilder;
    }


    private TrainingService mockTrainingService(String index, String trainType, Map<String, Object> mapping,
                                                Settings settings, List<ModelInputField> fields,
                                                ModelTargetField targetField,
                                                Function<AggregationBuilder, SearchRequestBuilder> mockSearchRequestBuilder)
            throws IOException {

        ClusterService clusterService = mock(ClusterService.class);
        MappingMetaData mappingMetaData = new MappingMetaData(trainType, mapping);
        IndexMetaData.Builder indexMetaData = IndexMetaData.builder(index)
                .putMapping(mappingMetaData)
                .putAlias(AliasMetaData.builder("just_me_alias"))
                .putAlias(AliasMetaData.builder("other_and_me_alias"))
                .settings(MINIMAL_INDEX_SETTINGS);
        IndexMetaData.Builder otherIndexMetaData = IndexMetaData.builder("other_index")
                .putMapping(mappingMetaData)
                .putAlias(AliasMetaData.builder("other_and_me_alias"))
                .settings(MINIMAL_INDEX_SETTINGS);
        MetaData.Builder metaData = MetaData.builder().put(indexMetaData).put(otherIndexMetaData);
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).metaData(metaData).build());

        AggregationBuilder aggregationBuilder = mock(AggregationBuilder.class);
        SearchRequestBuilder searchRequestBuilder = mockSearchRequestBuilder.apply(aggregationBuilder);

        Client client = mock(Client.class);

        when(client.prepareSearch()).thenReturn(searchRequestBuilder);
        when(client.prepareSearch()).thenReturn(searchRequestBuilder);

        SearchResponse searchResponse = mock(SearchResponse.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked") ActionListener<SearchResponse> listener =
                    (ActionListener<SearchResponse>) invocationOnMock.getArguments()[0];
            listener.onResponse(searchResponse);
            return null;
        }).when(searchRequestBuilder).execute(any());

        ModelTrainer modelTrainer = mock(ModelTrainer.class);
        when(modelTrainer.modelType()).thenReturn("mock");
        ModelTrainers modelTrainers = new ModelTrainers(Collections.singletonList(modelTrainer));
        TrainingSession trainingSession = mock(TrainingSession.class);
        when(trainingSession.trainingRequest()).thenReturn(aggregationBuilder);
        when(trainingSession.model(searchResponse)).thenReturn("Success!");
        when(modelTrainers.createTrainingSession(mappingMetaData, "mock", settings, fields, targetField)).thenReturn(trainingSession);
        return new TrainingService(Settings.EMPTY, clusterService, client, modelTrainers, searchParsers);

    }

    public void testPassingCorrectParameter() throws Exception {

        List<ModelInputField> fields = Arrays.asList(new ModelInputField("field1"), new ModelInputField("field2"));
        ModelTargetField targetField = new ModelTargetField("target");
        Settings settings = Settings.builder().put("foo", "bar").build();

        // Test single index
        TrainingService trainingService = mockTrainingService("train_index", "train_type", Collections.singletonMap("foo", "bar"),
                settings, fields, targetField, aggregationBuilder -> mockSearchRequestBuilder("train_index", aggregationBuilder));

        ExpectedListener listener = new ExpectedListener("Success!");
        trainingService.train("mock", settings, "train_index", "train_type", Collections.emptyMap(), fields, targetField, listener);
        listener.await();
    }

    public void testSingleIndexAlias() throws Exception {

        List<ModelInputField> fields = Arrays.asList(new ModelInputField("field1"), new ModelInputField("field2"));
        ModelTargetField targetField = new ModelTargetField("target");
        Settings settings = Settings.builder().put("foo", "bar").build();

        // Test single index alias
        TrainingService trainingService = mockTrainingService("train_index", "train_type", Collections.singletonMap("foo", "bar"),
                settings, fields, targetField, aggregationBuilder -> mockSearchRequestBuilder("just_me_alias", aggregationBuilder));
        ExpectedListener listener = new ExpectedListener("Success!");
        trainingService.train("mock", settings, "just_me_alias", "train_type", Collections.emptyMap(), fields, targetField, listener);
        listener.await();
    }

    public void testMultiIndexAlias() throws Exception {

        List<ModelInputField> fields = Arrays.asList(new ModelInputField("field1"), new ModelInputField("field2"));
        ModelTargetField targetField = new ModelTargetField("target");
        Settings settings = Settings.builder().put("foo", "bar").build();

        // Test single index alias
        TrainingService trainingService = mockTrainingService("train_index", "train_type", Collections.singletonMap("foo", "bar"),
                settings, fields, targetField, aggregationBuilder -> mockSearchRequestBuilder("other_and_me_alias", aggregationBuilder));
        ExpectedListener listener = new ExpectedListener(IllegalArgumentException.class, "can only train on a single index");
        trainingService.train("mock", settings, "other_and_me_alias", "train_type", Collections.emptyMap(), fields, targetField, listener);
        listener.await();
    }

    public void testMissingParameters() throws Exception {

        List<ModelInputField> fields = Arrays.asList(new ModelInputField("field1"), new ModelInputField("field2"));
        ModelTargetField targetField = new ModelTargetField("target");
        Settings settings = Settings.builder().put("foo", "bar").build();

        TrainingService trainingService = mockTrainingService("train_index", "train_type", Collections.singletonMap("foo", "bar"),
                settings, fields, targetField, aggregationBuilder -> mockSearchRequestBuilder("train_index", aggregationBuilder));

        ExpectedListener listener = new ExpectedListener(UnsupportedOperationException.class, "Unsupported model type [blah]");
        trainingService.train("blah", settings, "train_index", "train_type", Collections.emptyMap(), fields, targetField, listener);
        listener.await();

        listener = new ExpectedListener(IndexNotFoundException.class, "no such index");
        trainingService.train("mock", settings, "unknown_index", "train_type", Collections.emptyMap(), fields, targetField, listener);
        listener.await();

        listener = new ExpectedListener(ResourceNotFoundException.class, "the training type [unknown_type] not found");
        trainingService.train("mock", settings, "train_index", "unknown_type", Collections.emptyMap(), fields, targetField, listener);
        listener.await();
    }

    public void testPassingCustomTrainingQuery() throws Exception {

        List<ModelInputField> fields = Arrays.asList(new ModelInputField("field1"), new ModelInputField("field2"));
        ModelTargetField targetField = new ModelTargetField("target");
        Settings settings = Settings.builder().put("foo", "bar").build();

        QueryBuilder expectedQuery = QueryBuilders.termQuery("tag", "training");
        TrainingService trainingService = mockTrainingService("train_index", "train_type", Collections.singletonMap("foo", "bar"),
                settings, fields, targetField,
                aggregationBuilder -> mockSearchRequestBuilder("train_index", aggregationBuilder, expectedQuery));

        ExpectedListener listener = new ExpectedListener("Success!");
        Map<String, Object> query = Collections.singletonMap("term", Collections.singletonMap("tag", "training"));
        trainingService.train("mock", settings, "train_index", "train_type", query, fields, targetField, listener);
        listener.await();
    }



    private class ExpectedListener implements ActionListener<String> {

        private final CountDownLatch latch = new CountDownLatch(1);

        private final Matcher<Exception> failure;
        private final Matcher<String> failureMessage;
        private final Matcher<String> success;

        public ExpectedListener(Class<? extends Exception> clazz, String message) {
            this(instanceOf(clazz), equalTo(message), null);
        }

        public ExpectedListener(String success) {
            this(null, null, equalTo(success));
        }

        public ExpectedListener(Matcher<Exception> failure, Matcher<String> failureMessage, Matcher<String> success) {
            this.failure = failure;
            this.failureMessage = failureMessage;
            this.success = success;
        }

        @Override
        public void onResponse(String s) {
            try {
                if (success == null) {
                    fail("Expected exception, but got response [{" + s + "}]");
                }
                assertThat(s, success);
            } finally {
                latch.countDown();
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                if (failure == null) {
                    logger.error("Expected response, but got failure", e);
                    fail();
                }
                assertThat(e, failure);
                assertThat(e.getMessage(), failureMessage);
            } finally {
                latch.countDown();
            }

        }

        public void await() throws InterruptedException {
            latch.await(10, TimeUnit.SECONDS);
        }
    }


}
