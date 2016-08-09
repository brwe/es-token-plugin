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
import org.elasticsearch.script.models.ModelInput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class PMMLParser {

    private final Map<Class<? extends Model>, ModelParser<? extends ModelInput, ? extends Model>> modelParsers;

    public static PMMLParser createDefaultPMMLParser() {
        List<ModelParser<? extends ModelInput, ? extends Model>> parsers = new ArrayList<>();
        parsers.add(new GeneralizedLinearRegressionModelParser());
        parsers.add(new NaiveBayesModelParser());
        parsers.add(new TreeModelParser());
        return new PMMLParser(parsers);
    }

    public PMMLParser(List<ModelParser<? extends ModelInput, ? extends Model>> modelParsers) {
        Map<Class<? extends Model>, ModelParser<? extends ModelInput, ? extends Model>> modelParserMap = new HashMap<>();
        for (ModelParser<? extends ModelInput, ? extends Model> modelParser : modelParsers) {
            @SuppressWarnings("unchecked") ModelParser<? extends ModelInput, ? extends Model> prev =
                    modelParserMap.put(modelParser.getSupportedClass(), modelParser);
            if (prev != null) {
                throw new IllegalStateException("Added more than one parser for class " + modelParser.getSupportedClass());
            }
        }
        this.modelParsers = Collections.unmodifiableMap(modelParserMap);
    }

    @SuppressWarnings("unchecked")
    public <T extends ModelInput> ModelAndInputEvaluator<T> parse(PMML pmml, int modelNum) {
        Model model = pmml.getModels().get(modelNum);
        ModelParser<T, Model> modelParser = (ModelParser<T, Model>)modelParsers.get(model.getClass());
        if (modelParser != null) {
            return modelParser.parse(model, pmml.getDataDictionary(), pmml.getTransformationDictionary());
        } else {
            throw new UnsupportedOperationException("Unsupported model type " + model.getClass());
        }
    }

}
