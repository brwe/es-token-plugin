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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.ml.training.DataSet;
import org.elasticsearch.ml.training.ModelInputField;
import org.elasticsearch.ml.training.ModelTargetField;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TrainModelRequest extends ActionRequest<TrainModelRequest> {

    public static ObjectParser<TrainModelRequest, ParseFieldMatcherSupplier> PARSER =
            new ObjectParser<>("train_model_request", TrainModelRequest::new);

    static {
        PARSER.declareString(TrainModelRequest::setModelType, new ParseField("type"));
        PARSER.declareString(TrainModelRequest::setModelId, new ParseField("id"));
        PARSER.declareField(TrainModelRequest::setModelSettings, (p) -> Settings.builder().put(p.mapOrdered()).build(),
                new ParseField("settings"), ObjectParser.ValueType.OBJECT);
        PARSER.declareField(TrainModelRequest::setTargetField, (xContentParser, parseFieldMatcherSupplier) -> {
            if (xContentParser.currentToken() == XContentParser.Token.VALUE_STRING) {
                try {
                    return new ModelTargetField(xContentParser.text());
                } catch (IOException ex) {
                    throw new ElasticsearchParseException("cannot parse input field", ex);
                }
            }
            return ModelTargetField.PARSER.apply(xContentParser, parseFieldMatcherSupplier);
        }, new ParseField("target_field"), ObjectParser.ValueType.OBJECT_OR_STRING);
        PARSER.declareObjectArray(TrainModelRequest::setFields, (xContentParser, parseFieldMatcherSupplier) -> {
            if (xContentParser.currentToken() == XContentParser.Token.VALUE_STRING) {
                try {
                    return new ModelInputField(xContentParser.text());
                } catch (IOException ex) {
                    throw new ElasticsearchParseException("cannot parse input field", ex);
                }
            }
            return ModelInputField.PARSER.apply(xContentParser, parseFieldMatcherSupplier);
        }, new ParseField("fields"));
        PARSER.declareObject(TrainModelRequest::setTrainingSet, DataSet.PARSER, new ParseField("training_set"));
        PARSER.declareObject(TrainModelRequest::setTestingSet, DataSet.PARSER, new ParseField("testing_set"));
    }

    private String modelType;
    @Nullable
    private String modelId;
    private DataSet trainingSet;
    @Nullable
    private DataSet testingSet;

    private Settings modelSettings = Settings.EMPTY;
    private ModelTargetField outputField;
    private List<ModelInputField> fields = Collections.emptyList();


    public TrainModelRequest() {

    }

    public TrainModelRequest(String modelType, String modelId,
                             DataSet trainingSet, DataSet testingSet,
                             Settings modelSettings, ModelTargetField outputField,
                             ModelInputField... fields) {
        this.modelType = modelType;
        this.modelId = modelId;
        this.trainingSet = trainingSet;
        this.testingSet = testingSet;
        this.modelSettings = modelSettings;
        this.outputField = outputField;
        this.fields = Arrays.asList(fields);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (modelType == null) {
            validationException = ValidateActions.addValidationError("missing model type", validationException);
        }
        if (outputField == null) {
            validationException = ValidateActions.addValidationError("missing output field", validationException);
        }
        if (fields == null || fields.size() == 0) {
            validationException = ValidateActions.addValidationError("at least one input field is required", validationException);
        }
        if (modelSettings == null) {
            validationException = ValidateActions.addValidationError("missing model settings", validationException);
        }
        if (trainingSet == null) {
            validationException = ValidateActions.addValidationError("missing training set", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        modelType = in.readString();
        modelId = in.readOptionalString();
        trainingSet = new DataSet(in);
        testingSet = in.readOptionalWriteable(DataSet::new);
        modelSettings = Settings.readSettingsFromStream(in);
        outputField = new ModelTargetField(in);
        fields = in.readList(ModelInputField::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(modelType);
        out.writeOptionalString(modelId);
        trainingSet.writeTo(out);
        out.writeOptionalWriteable(testingSet);
        Settings.writeSettingsToStream(modelSettings, out);
        outputField.writeTo(out);
        out.writeList(fields);
    }

    public void source(BytesReference content) throws IOException {
        try (XContentParser parser = XContentHelper.createParser(content)) {
            TrainModelRequest.PARSER.parse(parser, this, () -> ParseFieldMatcher.STRICT);
        }
    }

    public void setModelType(String modelType) {
        this.modelType = modelType;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public void setModelSettings(Settings modelSettings) {
        this.modelSettings = modelSettings;
    }

    public void setTargetField(ModelTargetField outputField) {
        this.outputField = outputField;
    }

    public void setFields(List<ModelInputField> fields) {
        this.fields = fields;
    }

    public void setTrainingSet(DataSet trainingSet) {
        this.trainingSet = trainingSet;
    }

    public void setTestingSet(DataSet testingSet) {
        this.testingSet = testingSet;
    }

    public String getModelType() {
        return modelType;
    }

    public String getModelId() {
        return modelId;
    }

    public Settings getModelSettings() {
        return modelSettings;
    }

    public ModelTargetField getTargetField() {
        return outputField;
    }

    public List<ModelInputField> getFields() {
        return fields;
    }

    public DataSet getTrainingSet() {
        return trainingSet;
    }

    public DataSet getTestingSet() {
        return testingSet;
    }
}
