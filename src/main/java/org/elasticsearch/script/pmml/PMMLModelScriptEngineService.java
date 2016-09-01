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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorer;
import org.dmg.pmml.PMML;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ml.factories.ModelFactories;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.ml.modelinput.DataSource;
import org.elasticsearch.ml.modelinput.EsDataSource;
import org.elasticsearch.ml.models.EsModelEvaluator;
import org.elasticsearch.ml.modelinput.ModelAndModelInputEvaluator;
import org.elasticsearch.ml.modelinput.ModelInput;
import org.elasticsearch.ml.modelinput.ModelInputEvaluator;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafIndexLookup;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

/**
 * Provides the infrastructure for Groovy as a scripting language for Elasticsearch
 */
public class PMMLModelScriptEngineService extends AbstractComponent implements ScriptEngineService {

    public static final String NAME = "pmml_model";

    public static final ModelFactories factories = ModelFactories.createDefaultModelFactories();

    @Inject
    public PMMLModelScriptEngineService(Settings settings) {
        super(settings);
    }

    @Override
    public void close() {
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public String getExtension() {
        return NAME;
    }

    @Override
    public Object compile(String scriptName, String scriptSource, Map<String, String> params) {
        return new Factory<>(scriptSource);
    }

    @Override
    public ExecutableScript executable(CompiledScript compiledScript, @Nullable Map<String, Object> vars) {
        throw new UnsupportedOperationException("model script not supported in this context!");
    }

    public class Factory<Input extends ModelInput, Output> {
        public EsModelEvaluator<Input, Output> getModel() {
            return model;
        }

        ModelInputEvaluator<Input> features = null;

        private EsModelEvaluator<Input, Output> model;

        @SuppressWarnings("unchecked")
        public Factory(String spec) {
            ModelAndModelInputEvaluator<Input, Output> fieldsToVectorAndModel = parsePMML(spec);
            features = fieldsToVectorAndModel.getVectorRangesToVector();
            model = fieldsToVectorAndModel.getModel();
        }

        private ModelAndModelInputEvaluator<Input, Output> parsePMML(final String pmmlString) {
            PMML pmml = ProcessPMMLHelper.parsePmml(pmmlString);
            if (pmml.getModels().size() > 1) {
                throw new UnsupportedOperationException("Only implemented PMML for one model so far.");
            }
            return factories.buildFromPMML(pmml, 0);
        }

        public PMMLModel<Input, Output> newScript(LeafSearchLookup lookup, boolean debug) {
            return new PMMLModel<>(features, model, lookup, debug);
        }
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public SearchScript search(final CompiledScript compiledScript, final SearchLookup lookup, @Nullable final Map<String, Object> vars) {
        return new SearchScript() {

            @Override
            public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
                final LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);
                boolean debug = true;
                if (vars != null && vars.containsKey("debug")) {
                    debug = (Boolean)vars.get("debug");
                }
                return ((Factory) compiledScript.compiled()).newScript(leafLookup, debug);
            }

            @Override
            public boolean needsScores() {
                // TODO: can we reliably know if a vectorizer script does not make use of _score
                return false;
            }
        };
    }

    public static class PMMLModel<Input extends ModelInput, Output> implements LeafSearchScript {
        EsModelEvaluator<Input, Output> model = null;
        private boolean debug;
        private final ModelInputEvaluator<Input> features;
        private LeafSearchLookup lookup;
        private DataSource dataSource;

        private PMMLModel(ModelInputEvaluator<Input> features, EsModelEvaluator<Input, Output> model,
                          LeafSearchLookup lookup, boolean debug) {
            this.dataSource = new EsDataSource() {
                @Override
                protected LeafDocLookup getDocLookup() {
                    return lookup.doc();
                }

                @Override
                protected LeafIndexLookup getLeafIndexLookup() {
                    return lookup.indexLookup();
                }
            };
            this.lookup = lookup;
            this.features = features;
            this.model = model;
            this.debug = debug;
        }

        @Override
        public void setNextVar(String s, Object o) {
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object run() {
            Input vector = features.convert(dataSource);
            if (debug) {
                return model.evaluateDebug(vector);
            } else {
                return model.evaluate(vector);
            }
        }

        @Override
        public Object unwrap(Object o) {
            return o;
        }

        @Override
        public void setDocument(int i) {
            if (lookup != null) {
                lookup.setDocument(i);
            }
        }

        @Override
        public void setSource(Map<String, Object> map) {
            if (lookup != null) {
                lookup.source().setSource(map);
            }
        }

        @Override
        public long runAsLong() {
            throw new UnsupportedOperationException("model script not supported in this context!");
        }

        @Override
        public double runAsDouble() {
            throw new UnsupportedOperationException("model script not supported in this context!");
        }

        @Override
        public void setScorer(Scorer scorer) {

        }
    }
}

