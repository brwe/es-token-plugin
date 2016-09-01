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

package org.elasticsearch.action.trainmodel;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ml.training.DataSet;
import org.elasticsearch.ml.training.ModelInputField;
import org.elasticsearch.ml.training.ModelTargetField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TrainModelRequestBuilder extends ActionRequestBuilder<TrainModelRequest, TrainModelResponse,
        TrainModelRequestBuilder> {

    public TrainModelRequestBuilder(ElasticsearchClient client) {
        super(client, TrainModelAction.INSTANCE, new TrainModelRequest());
    }

    public TrainModelRequestBuilder source(BytesReference source) throws IOException {
        request.source(source);
        return this;
    }

    @Override
    public void execute(ActionListener<TrainModelResponse> listener) {
        client.execute(TrainModelAction.INSTANCE, request, listener);
    }

    public TrainModelRequestBuilder setModelId(String id) {
        request.setModelId(id);
        return this;
    }

    public TrainModelRequestBuilder setModelType(String modelType) {
        request.setModelType(modelType);
        return this;
    }

    public TrainModelRequestBuilder addFields(ModelInputField ... inputFields) {
        List<ModelInputField> newFields = new ArrayList<>();
        if (request.getFields() == null) {
            newFields.addAll(request.getFields());
        }
        for (ModelInputField modelInputField : inputFields) {
            newFields.add(modelInputField);
        }
        request.setFields(Collections.unmodifiableList(newFields));
        return this;
    }

    public TrainModelRequestBuilder addFields(String ... inputFields) {
        List<ModelInputField> newFields = new ArrayList<>();
        if (request.getFields() == null) {
            newFields.addAll(request.getFields());
        }
        for (String modelInputField : inputFields) {
            newFields.add(new ModelInputField(modelInputField));
        }
        request.setFields(Collections.unmodifiableList(newFields));
        return this;
    }

    public TrainModelRequestBuilder setTargetField(ModelTargetField targetField) {
        request.setTargetField(targetField);
        return this;
    }

    public TrainModelRequestBuilder setTargetField(String targetField) {
        request.setTargetField(new ModelTargetField(targetField));
        return this;
    }

    public TrainModelRequestBuilder setSettings(Settings settings) {
        request.setModelSettings(settings);
        return this;
    }

    public TrainModelRequestBuilder setTrainingSet(DataSet dataSet) {
        request.setTrainingSet(dataSet);
        return this;
    }

    public TrainModelRequestBuilder setTestingSet(DataSet dataSet) {
        request.setTestingSet(dataSet);
        return this;
    }

}
