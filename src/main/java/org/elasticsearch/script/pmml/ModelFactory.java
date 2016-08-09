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

import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.Model;
import org.dmg.pmml.TransformationDictionary;
import org.elasticsearch.script.modelinput.ModelAndModelInputEvaluator;
import org.elasticsearch.script.modelinput.ModelInput;

/**
 *
 */
public abstract class ModelFactory<T extends ModelInput, M extends Model> {

    protected ModelFactory(Class<M> supportedClass) {
        this.supportedClass = supportedClass;
    }

    private final Class<M> supportedClass;

    public Class<M> getSupportedClass() {
        return supportedClass;
    }

    public abstract ModelAndModelInputEvaluator<T> buildFromPMML(M model, DataDictionary dataDictionary,
                                                                 TransformationDictionary transformationDictionary);

}
