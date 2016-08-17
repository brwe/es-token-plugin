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

import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.elasticsearch.script.modelinput.ModelAndModelInputEvaluator;
import org.elasticsearch.script.modelinput.ModelInput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ModelFactories {

    private final Map<Class<? extends Model>, ModelFactory<? extends ModelInput, ?, ? extends Model>> modelFactories;

    public static ModelFactories createDefaultModelFactories() {
        List<ModelFactory<? extends ModelInput, ?, ? extends Model>> parsers = new ArrayList<>();
        parsers.add(new GeneralizedLinearRegressionModelFactory());
        parsers.add(new NaiveBayesModelFactory());
        parsers.add(new TreeModelFactory());
        return new ModelFactories(parsers);
    }

    public ModelFactories(List<ModelFactory<? extends ModelInput, ?, ? extends Model>> modelFactories) {
        Map<Class<? extends Model>, ModelFactory<? extends ModelInput, ?, ? extends Model>> modelParserMap = new HashMap<>();
        for (ModelFactory<? extends ModelInput, ?, ? extends Model> modelFactory : modelFactories) {
            @SuppressWarnings("unchecked") ModelFactory<? extends ModelInput, ?, ? extends Model> prev =
                    modelParserMap.put(modelFactory.getSupportedClass(), modelFactory);
            if (prev != null) {
                throw new IllegalStateException("Added more than one factories for class " + modelFactory.getSupportedClass());
            }
        }
        this.modelFactories = Collections.unmodifiableMap(modelParserMap);
    }

    @SuppressWarnings("unchecked")
    public <Input extends ModelInput, Output> ModelAndModelInputEvaluator<Input, Output> buildFromPMML(PMML pmml, int modelNum) {
        Model model = pmml.getModels().get(modelNum);
        ModelFactory<Input, Output, Model> modelFactory = (ModelFactory<Input, Output, Model>) modelFactories.get(model.getClass());
        if (modelFactory != null) {
            return modelFactory.buildFromPMML(model, pmml.getDataDictionary(), pmml.getTransformationDictionary());
        } else {
            throw new UnsupportedOperationException("Unsupported model type " + model.getClass());
        }
    }

}
