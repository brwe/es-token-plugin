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
import org.dmg.pmml.GeneralRegressionModel;
import org.dmg.pmml.Model;
import org.dmg.pmml.NaiveBayesModel;
import org.dmg.pmml.PMML;
import org.dmg.pmml.RegressionModel;
import org.dmg.pmml.TreeModel;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.modelinput.VectorRangesToVector;
import org.elasticsearch.script.modelinput.VectorRangesToVectorJSON;
import org.elasticsearch.script.models.EsLinearSVMModel;
import org.elasticsearch.script.models.EsLogisticRegressionModel;
import org.elasticsearch.script.models.EsModelEvaluator;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.Map;

/**
 * Provides the infrastructure for Groovy as a scripting language for Elasticsearch
 */
public class PMMLModelScriptEngineService extends AbstractComponent implements ScriptEngineService {

    public static final String NAME = "pmml_model";

    @Inject
    public PMMLModelScriptEngineService(Settings settings) {
        super(settings);
    }

    @Override
    public void close() {
    }

    @Override
    public void scriptRemoved(@Nullable CompiledScript script) {
    }

    @Override
    public String[] types() {
        return new String[]{NAME};
    }

    @Override
    public String[] extensions() {
        return new String[]{NAME};
    }

    @Override
    public boolean sandboxed() {
        return false;
    }

    @Override
    public Object compile(String script) {
        return new Factory(script);
    }

    @Override
    public ExecutableScript executable(CompiledScript compiledScript, @Nullable Map<String, Object> vars) {
        throw new UnsupportedOperationException("model script not supported in this context!");
    }

    public static class FieldsToVectorAndModel {
        public FieldsToVectorAndModel(VectorRangesToVector vectorRangesToVector, EsModelEvaluator model) {
            this.vectorRangesToVector = vectorRangesToVector;
            this.model = model;
        }

        public VectorRangesToVector getVectorRangesToVector() {
            return vectorRangesToVector;
        }

        final VectorRangesToVector vectorRangesToVector;

        public EsModelEvaluator getModel() {
            return model;
        }

        final EsModelEvaluator model;
    }

    public static class Factory {
        public static final String VECTOR_MODEL_DELIMITER = "dont know what to put here";

        public VectorRangesToVector getFeatures() {
            return features;
        }

        public EsModelEvaluator getModel() {
            return model;
        }

        VectorRangesToVector features = null;

        private EsModelEvaluator model;

        public Factory(String spec) {
            if (spec.contains(VECTOR_MODEL_DELIMITER)) {
                // In case someone pulled the vectors from elasticsearch the the vector spec is stored in the same script
                // as the model but as a json string
                // this is a clumsy workaround which we probably should remove at some point.
                // Would be much better if we figure out TextIndex in PMML:
                // http://dmg.org/pmml/v4-2-1/Transformations.html#xsdElement_TextIndex
                // or we remove the ability to pull vectors from elasticsearch via this plugin altogether...

                // split into vector and model
                String[] vectorAndModel = spec.split(VECTOR_MODEL_DELIMITER);
                Map<String, Object> parsedSource = null;
                try {
                    XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(vectorAndModel[0]);
                    parsedSource = parser.mapOrdered();
                } catch (IOException e) {
                    throw new ScriptException("pmml prediction failed", e);
                }
                features = new VectorRangesToVectorJSON(parsedSource);

                if (model == null) {
                    try {
                        model = initModelWithoutPreProcessing(vectorAndModel[1]);


                    } catch (IOException e) {
                        throw new ScriptException("pmml prediction failed", e);
                    } catch (SAXException e) {
                        throw new ScriptException("pmml prediction failed", e);
                    } catch (JAXBException e) {
                        throw new ScriptException("pmml prediction failed", e);
                    }
                }
            } else {
                FieldsToVectorAndModel fieldsToVectorAndModel = initFeaturesAndModelFromFullPMMLSpec(spec);
                features = fieldsToVectorAndModel.vectorRangesToVector;
                model = fieldsToVectorAndModel.model;
            }
        }

        static private FieldsToVectorAndModel initFeaturesAndModelFromFullPMMLSpec(final String pmmlString) {

            PMML pmml = ProcessPMMLHelper.parsePmml(pmmlString);
            if (pmml.getModels().size() > 1) {
                throw new UnsupportedOperationException("Only implemented PMML for one model so far.");
            }
            return getFeaturesAndModelFromFullPMMLSpec(pmml, 0);

        }

        public static EsModelEvaluator initModelWithoutPreProcessing(final String pmmlString) throws IOException, SAXException, JAXBException {
            // this is bad but I have not figured out yet how to avoid the permission for suppressAccessCheck
            PMML pmml = ProcessPMMLHelper.parsePmml(pmmlString);
            Model model = pmml.getModels().get(0);
            if (model.getModelName().equals("logistic regression")) {
                return initLogisticRegression((RegressionModel) model);
            } else if (model.getModelName().equals("linear SVM")) {
                return initLinearSVM((RegressionModel) model);
            } else {
                throw new UnsupportedOperationException("We only implemented logistic regression so far but your model is of type " + model.getModelName());
            }

        }

        protected static EsModelEvaluator initLogisticRegression(RegressionModel pmmlModel) {
            return new EsLogisticRegressionModel(pmmlModel);
        }

        protected static EsModelEvaluator initLinearSVM(RegressionModel pmmlModel) {
            return new EsLinearSVMModel(pmmlModel);
        }


        public PMMLModel newScript(LeafSearchLookup lookup, boolean debug) {
            return new PMMLModel(features, model, lookup, debug);
        }
    }

    public static FieldsToVectorAndModel getFeaturesAndModelFromFullPMMLSpec(PMML pmml, int modelNum) {

        Model model = pmml.getModels().get(modelNum);
        if (model instanceof GeneralRegressionModel) {
            return GeneralizedLinearRegressionHelper.getGeneralRegressionFeaturesAndModel(pmml, modelNum);

        } else if (model instanceof TreeModel) {
            return TreeModelHelper.getTreeModelFeaturesAndModel(pmml, modelNum);
        } else if (model instanceof NaiveBayesModel) {
            return NaiveBayesModelHelper.getNaiveBayesFeaturesAndModel(pmml, modelNum);
        } else {
            throw new UnsupportedOperationException("Only implemented general regression model so far.");
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
                if (vars.containsKey("debug")) {
                    debug = (Boolean)vars.get("debug");
                }
                PMMLModel scriptObject = ((Factory) compiledScript.compiled()).newScript(leafLookup, debug);
                return scriptObject;
            }

            @Override
            public boolean needsScores() {
                // TODO: can we reliably know if a vectorizer script does not make use of _score
                return false;
            }
        };
    }

    public static class PMMLModel implements LeafSearchScript {
        EsModelEvaluator model = null;
        private boolean debug;
        private final VectorRangesToVector features;
        private LeafSearchLookup lookup;

        /**
         * Factory that is registered in
         * {@link TokenPlugin#onModule(ScriptModule)}
         * method when the plugin is loaded.
         */

        /**
         * @throws ScriptException
         */
        private PMMLModel(VectorRangesToVector features, EsModelEvaluator model, LeafSearchLookup lookup, boolean debug) throws
                ScriptException {

            this.lookup = lookup;
            this.features = features;
            this.model = model;
            this.debug = debug;
        }

        @Override
        public void setNextVar(String s, Object o) {
        }

        @Override
        public Object run() {
            Object vector = features.vector(lookup.doc(), lookup.fields(), lookup.indexLookup(), lookup.source());
            assert vector instanceof Map;
            if (debug) {
                return model.evaluateDebug((Map<String, Object>) vector);
            } else {
                return model.evaluate((Map<String, Object>) vector);
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
        public float runAsFloat() {
            throw new UnsupportedOperationException("model script not supported in this context!");
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

