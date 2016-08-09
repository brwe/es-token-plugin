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
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.script.modelinput.DataSource;
import org.elasticsearch.script.modelinput.EsDataSource;
import org.elasticsearch.script.models.EsModelEvaluator;
import org.elasticsearch.script.models.ModelInput;
import org.elasticsearch.script.models.ModelInputEvaluator;
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

    public static final PMMLParser parser = PMMLParser.createDefaultPMMLParser();

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

    public class Factory<T extends ModelInput> {
        public EsModelEvaluator<T> getModel() {
            return model;
        }

        ModelInputEvaluator<T> features = null;

        private EsModelEvaluator<T> model;

        @SuppressWarnings("unchecked")
        public Factory(String spec) {
            ModelAndInputEvaluator<T> fieldsToVectorAndModel = initFeaturesAndModelFromFullPMMLSpec(spec);
            features = fieldsToVectorAndModel.vectorRangesToVector;
            model = fieldsToVectorAndModel.model;
        }

        private ModelAndInputEvaluator<T> initFeaturesAndModelFromFullPMMLSpec(final String pmmlString) {

            PMML pmml = ProcessPMMLHelper.parsePmml(pmmlString);
            if (pmml.getModels().size() > 1) {
                throw new UnsupportedOperationException("Only implemented PMML for one model so far.");
            }
            return getFeaturesAndModelFromFullPMMLSpec(pmml, 0);

        }

        public PMMLModel<T> newScript(LeafSearchLookup lookup, boolean debug) {
            return new PMMLModel<>(features, model, lookup, debug);
        }
    }

    public <T extends ModelInput> ModelAndInputEvaluator<T> getFeaturesAndModelFromFullPMMLSpec(PMML pmml, int modelNum) {
        return parser.parse(pmml, modelNum);
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public SearchScript search(final CompiledScript compiledScript, final SearchLookup lookup, @Nullable final Map<String, Object> vars) {
        return new SearchScript() {

            @Override
            public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
                final LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);
                boolean debug = true;
                if (vars.containsKey("debug")) {
                    debug = (Boolean)vars.get("debug");
                }
                PMMLModel<? extends ModelInput> scriptObject = ((Factory) compiledScript.compiled()).newScript(leafLookup, debug);
                return scriptObject;
            }

            @Override
            public boolean needsScores() {
                // TODO: can we reliably know if a vectorizer script does not make use of _score
                return false;
            }
        };
    }

    public static class PMMLModel<T extends ModelInput> implements LeafSearchScript {
        EsModelEvaluator<T> model = null;
        private boolean debug;
        private final ModelInputEvaluator<T> features;
        private LeafSearchLookup lookup;
        private DataSource dataSource;

        /**
         * Factory that is registered in
         * {@link TokenPlugin#onModule(ScriptModule)}
         * method when the plugin is loaded.
         */

        private PMMLModel(ModelInputEvaluator<T> features, EsModelEvaluator<T> model, LeafSearchLookup lookup, boolean debug) {
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
            T vector = features.convert(dataSource);
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

