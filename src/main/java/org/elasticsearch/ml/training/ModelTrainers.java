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

import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ml.training.ModelTrainer.TrainingSession;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Collection of all available model trainers
 */
public class ModelTrainers {
    private final Map<String, ModelTrainer> modelTrainers;

    public ModelTrainers(List<ModelTrainer> modelTrainers) {
        Map<String, ModelTrainer> modelParserMap = new HashMap<>();
        for (ModelTrainer trainer : modelTrainers) {
            ModelTrainer prev = modelParserMap.put(trainer.modelType(), trainer);
            if (prev != null) {
                throw new IllegalStateException("Added more than one trainer for model type [" + trainer.modelType() + "]");
            }
        }
        this.modelTrainers = Collections.unmodifiableMap(modelParserMap);
    }

    public TrainingSession createTrainingSession(MappingMetaData mappingMetaData, String modelType, Settings settings,
                                                 List<ModelInputField> inputs, ModelTargetField output) {
        ModelTrainer trainer = modelTrainers.get(modelType);
        if (trainer != null) {
            return trainer.createTrainingSession(mappingMetaData, inputs, output, settings);
        } else {
            throw new UnsupportedOperationException("Unsupported model type [" + modelType + "]");
        }
    }
}
