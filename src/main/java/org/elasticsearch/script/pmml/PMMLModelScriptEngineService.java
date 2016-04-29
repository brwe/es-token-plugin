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
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.dmg.pmml.RegressionModel;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.script.*;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.transform.Source;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedAction;
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
        return new String[0];
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

    public static class FeaturesAndModel {
        VectorEntries features;
        EsModelEvaluator model;
    }

    public static class Factory {
        public static final String VECTOR_MODEL_DELIMITER = "dont know what to put here";

        public VectorEntries getFeatures() {
            return features;
        }

        public EsModelEvaluator getModel() {
            return model;
        }

        VectorEntries features = null;

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
                features = new VectorEntriesJSON(parsedSource);

                if (model == null) {
                    try {
                        model = initModel(vectorAndModel[1]);


                    } catch (IOException e) {
                        throw new ScriptException("pmml prediction failed", e);
                    } catch (SAXException e) {
                        throw new ScriptException("pmml prediction failed", e);
                    } catch (JAXBException e) {
                        throw new ScriptException("pmml prediction failed", e);
                    }
                }
            } else {
                FeaturesAndModel featuresAndModel = initFeaturesAndModel(spec);
                features = featuresAndModel.features;
                model = featuresAndModel.model;
            }
        }

        static private FeaturesAndModel initFeaturesAndModel(final String pmmlString) {
            // this is bad but I have not figured out yet how to avoid the permission for suppressAccessCheck
            PMML pmml = AccessController.doPrivileged(new PrivilegedAction<PMML>() {
                public PMML run() {
                    try (InputStream is = new ByteArrayInputStream(pmmlString.getBytes(Charset.defaultCharset()))) {
                        Source transformedSource = ImportFilter.apply(new InputSource(is));
                        return JAXBUtil.unmarshalPMML(transformedSource);
                    } catch (SAXException e) {
                        throw new ElasticsearchException("could not convert xml to pmml model", e);
                    } catch (JAXBException e) {
                        throw new ElasticsearchException("could not convert xml to pmml model", e);
                    } catch (IOException e) {
                        throw new ElasticsearchException("could not convert xml to pmml model", e);
                    }
                }
            });
            if (pmml.getModels().size() > 1) {
                throw new UnsupportedOperationException("Only implemented PMML for one model so far.");
            }
            VectorEntries features = new VectorEntriesPMML(pmml, 0);
            Model model = pmml.getModels().get(0);
            return null;

        }

        public static EsModelEvaluator initModel(final String pmmlString) throws IOException, SAXException, JAXBException {
            // this is bad but I have not figured out yet how to avoid the permission for suppressAccessCheck
            PMML pmml = AccessController.doPrivileged(new PrivilegedAction<PMML>() {
                public PMML run() {
                    try (InputStream is = new ByteArrayInputStream(pmmlString.getBytes(Charset.defaultCharset()))) {
                        Source transformedSource = ImportFilter.apply(new InputSource(is));
                        return JAXBUtil.unmarshalPMML(transformedSource);
                    } catch (SAXException e) {
                        throw new ElasticsearchException("could not convert xml to pmml model", e);
                    } catch (JAXBException e) {
                        throw new ElasticsearchException("could not convert xml to pmml model", e);
                    } catch (IOException e) {
                        throw new ElasticsearchException("could not convert xml to pmml model", e);
                    }
                }
            });
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


        public PMMLModel newScript(LeafSearchLookup lookup) {
            return new PMMLModel(features, model, lookup);
        }
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public SearchScript search(final CompiledScript compiledScript, final SearchLookup lookup, @Nullable final Map<String, Object> vars) {
        return new SearchScript() {

            @Override
            public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
                final LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);
                PMMLModel scriptObject = ((Factory) compiledScript.compiled()).newScript(leafLookup);
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
        private final VectorEntries features;
        private LeafSearchLookup lookup;

        /**
         * Factory that is registered in
         * {@link TokenPlugin#onModule(ScriptModule)}
         * method when the plugin is loaded.
         */

        /**
         * @throws ScriptException
         */
        private PMMLModel(VectorEntries features, EsModelEvaluator model, LeafSearchLookup lookup) throws ScriptException {

            this.lookup = lookup;
            this.features = features;
            this.model = model;
        }

        @Override
        public void setNextVar(String s, Object o) {
        }

        @Override
        public Object run() {
            Object vector = features.vector(lookup.doc(), lookup.fields(), lookup.indexLookup(), lookup.source());
            assert vector instanceof Map;
            if (features.isSparse() == false) {
                Map<String, Object> denseVector = (Map<String, Object>) vector;
                assert (denseVector.get("values") instanceof double[]);
                return model.evaluate((double[]) denseVector.get("values"));
            } else {
                Map<String, Object> sparseVector = (Map<String, Object>) vector;
                assert (sparseVector.get("indices") instanceof int[]);
                assert (sparseVector.get("values") instanceof double[]);
                Tuple<int[], double[]> indicesAndValues = new Tuple<>((int[]) sparseVector.get("indices"), (double[]) sparseVector.get("values"));
                return model.evaluate(indicesAndValues);
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

