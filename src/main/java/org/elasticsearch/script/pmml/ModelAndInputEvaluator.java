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

package org.elasticsearch.script.pmml;

import org.elasticsearch.script.models.EsModelEvaluator;
import org.elasticsearch.script.models.ModelInput;
import org.elasticsearch.script.models.ModelInputEvaluator;

/**
 *
 */
public class ModelAndInputEvaluator<T extends ModelInput> {
    public ModelAndInputEvaluator(ModelInputEvaluator<T> vectorRangesToVector, EsModelEvaluator<T> model) {
        this.vectorRangesToVector = vectorRangesToVector;
        this.model = model;
    }

    public ModelInputEvaluator<T> getVectorRangesToVector() {
        return vectorRangesToVector;
    }

    final ModelInputEvaluator<T> vectorRangesToVector;

    public EsModelEvaluator<T> getModel() {
        return model;
    }

    final EsModelEvaluator<T> model;
}
