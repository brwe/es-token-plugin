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

import org.elasticsearch.action.trainmodel.TrainModelRequest;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

/**
 */
public class TrainingRequestParsingTests extends ESTestCase {

    public void testMinimalModel() throws Exception {
        TrainModelRequest trainModelRequest = new TrainModelRequest();
        XContentBuilder sourceBuilder = jsonBuilder();

        sourceBuilder.startObject();
        {
            sourceBuilder.field("id", "abcd");
            sourceBuilder.array("fields", "field1", "field2", "field3");
            sourceBuilder.field("target_field", "class");
            sourceBuilder.startObject("training_set");
            {
                sourceBuilder.field("index", "test");
                sourceBuilder.field("type", "type");
            }
            sourceBuilder.endObject();
        }
        sourceBuilder.endObject();

        trainModelRequest.source(sourceBuilder.bytes());

        assertThat(trainModelRequest.getModelId(), equalTo("abcd"));
        assertThat(trainModelRequest.getFields(), equalTo(
                Arrays.asList(new ModelInputField("field1"), new ModelInputField("field2"), new ModelInputField("field3"))));
        assertThat(trainModelRequest.getTargetField(), equalTo(new ModelTargetField("class")));
        assertThat(trainModelRequest.getTrainingSet(), equalTo(new DataSet("test", "type")));
        assertThat(trainModelRequest.getModelSettings(), equalTo(Settings.EMPTY));
    }

    public void testModelWithCustomSettings() throws Exception {
        TrainModelRequest trainModelRequest = new TrainModelRequest();
        XContentBuilder sourceBuilder = jsonBuilder();

        sourceBuilder.startObject();
        {
            sourceBuilder.field("id", "abcd");
            sourceBuilder.startArray("fields");
            {
                sourceBuilder.startObject().field("name", "field1").endObject();
                sourceBuilder.startObject().field("name", "field2").endObject();
                sourceBuilder.startObject().field("name", "field3").endObject();
            }
            sourceBuilder.endArray();
            sourceBuilder.startObject("target_field").field("name", "class").endObject();
            sourceBuilder.startObject("training_set");
            {
                sourceBuilder.field("index", "test");
                sourceBuilder.field("type", "type");
            }
            sourceBuilder.endObject();
            sourceBuilder.startObject("settings");
            {
                sourceBuilder.field("foo", "bar");
                sourceBuilder.field("bar", "baz");
            }
            sourceBuilder.endObject();
        }
        sourceBuilder.endObject();

        trainModelRequest.source(sourceBuilder.bytes());

        assertThat(trainModelRequest.getModelId(), equalTo("abcd"));
        assertThat(trainModelRequest.getFields(), equalTo(
                Arrays.asList(new ModelInputField("field1"), new ModelInputField("field2"), new ModelInputField("field3"))));
        assertThat(trainModelRequest.getTargetField(), equalTo(new ModelTargetField("class")));
        assertThat(trainModelRequest.getTrainingSet(), equalTo(new DataSet("test", "type")));
        assertThat(trainModelRequest.getModelSettings(), equalTo(Settings.builder().put("foo", "bar").put("bar","baz").build()));
    }


    public void testModelWithTrainingQuery() throws Exception {
        TrainModelRequest trainModelRequest = new TrainModelRequest();
        XContentBuilder sourceBuilder = jsonBuilder();

        sourceBuilder.startObject();
        {
            sourceBuilder.field("id", "abcd");
            sourceBuilder.array("fields", "field1", "field2", "field3");
            sourceBuilder.field("target_field", "class");
            sourceBuilder.startObject("training_set");
            {
                sourceBuilder.field("index", "test");
                sourceBuilder.field("type", "type");
            }
            sourceBuilder.endObject();
            sourceBuilder.startObject("training_set");
            {
                sourceBuilder.field("index", "test");
                sourceBuilder.field("type", "type");
                sourceBuilder.startObject("query").startObject("match_all").endObject().endObject();
            }
            sourceBuilder.endObject();
        }
        sourceBuilder.endObject();

        trainModelRequest.source(sourceBuilder.bytes());

        assertThat(trainModelRequest.getModelId(), equalTo("abcd"));
        assertThat(trainModelRequest.getFields(), equalTo(
                Arrays.asList(new ModelInputField("field1"), new ModelInputField("field2"), new ModelInputField("field3"))));
        assertThat(trainModelRequest.getTargetField(), equalTo(new ModelTargetField("class")));
        Map<String, Object> matchAllQuery = Collections.singletonMap("match_all", Collections.emptyMap());
        assertThat(trainModelRequest.getTrainingSet(), equalTo(new DataSet("test", "type", matchAllQuery)));
    }
}
