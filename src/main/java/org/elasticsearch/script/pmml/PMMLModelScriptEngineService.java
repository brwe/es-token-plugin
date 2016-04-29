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
import org.dmg.pmml.Apply;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.Expression;
import org.dmg.pmml.FieldRef;
import org.dmg.pmml.GeneralRegressionModel;
import org.dmg.pmml.Model;
import org.dmg.pmml.NormContinuous;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.PPCell;
import org.dmg.pmml.Parameter;
import org.dmg.pmml.Predictor;
import org.dmg.pmml.RegressionModel;
import org.dmg.pmml.TransformationDictionary;
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
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.EsLinearSVMModel;
import org.elasticsearch.script.EsLogisticRegressionModel;
import org.elasticsearch.script.EsModelEvaluator;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.FeatureEntries;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.PMMLFeatureEntries;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.script.VectorEntries;
import org.elasticsearch.script.VectorEntriesJSON;
import org.elasticsearch.script.VectorEntriesPMML;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

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
        public FeaturesAndModel(VectorEntries features, EsModelEvaluator model) {
            this.features = features;
            this.model = model;
        }

        public VectorEntries getFeatures() {
            return features;
        }

        final VectorEntries features;

        public EsModelEvaluator getModel() {
            return model;
        }

        final EsModelEvaluator model;
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
                FeaturesAndModel featuresAndModel = initFeaturesAndModelFromFullPMMLSpec(spec);
                features = featuresAndModel.features;
                model = featuresAndModel.model;
            }
        }

        static private FeaturesAndModel initFeaturesAndModelFromFullPMMLSpec(final String pmmlString) {
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
            return getFeaturesAndModelFromFullPMMLSpec(pmml, 0);

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

    public static FeaturesAndModel getFeaturesAndModelFromFullPMMLSpec(PMML pmml, int modelNum) {

        Model model = pmml.getModels().get(modelNum);
        if (model instanceof GeneralRegressionModel) {

            return getGeneralRegressionFeaturesAndModel(pmml, modelNum);

        } else {
            throw new UnsupportedOperationException("Only implemented general regression model so far.");
        }

    }

    private static FeaturesAndModel getGeneralRegressionFeaturesAndModel(PMML pmml, int modelNum) {
        GeneralRegressionModel grModel = (GeneralRegressionModel) pmml.getModels().get(modelNum);
        if (grModel.getAlgorithmName().equals("LogisticRegression") && grModel.getModelType().value().equals
                ("multinomialLogistic")) {
            TreeMap<String, List<PPCell>> fieldToPPCellMap = mapCellsToFields(grModel);
            List<String> orderedParameterList = new ArrayList<>();
            List<FeatureEntries> featureEntriesList = convertToFeatureEntries(pmml, modelNum, fieldToPPCellMap, orderedParameterList);
            //add intercept if any
            addIntercept(grModel, featureEntriesList, fieldToPPCellMap, orderedParameterList);

            assert orderedParameterList.size() == grModel.getParameterList().getParameters().size();
            VectorEntriesPMML vectorEntries = createGeneralizedRegressionModelVectorEntries(featureEntriesList, orderedParameterList
                    .toArray(new String[orderedParameterList.size()]));

            // now finally create the model!
            return new FeaturesAndModel(vectorEntries, null);

        } else {
            throw new UnsupportedOperationException("Only implemented logistic regression with multinomialLogistic so far.");
        }
    }

    private static void addIntercept(GeneralRegressionModel grModel, List<FeatureEntries> featureEntriesMap, Map<String, List<PPCell>>
            fieldToPPCellMap, List<String> orderedParameterList) {
        // now, find the order of vector entries to model parameters. This is extremely annoying but we have to do it at some
        // point...


        int numFeatures = 0; // current index?
        Set<String> allFieldParameters = new HashSet<>();
        for (Map.Entry<String, List<PPCell>> fieldAndCells : fieldToPPCellMap.entrySet()) {
            for (PPCell cell : fieldAndCells.getValue()) {
                allFieldParameters.add(cell.getParameterName());
                numFeatures++;
            }
        }
        // now find the parameters which do not come form a field
        for (Parameter parameter : grModel.getParameterList().getParameters()) {
            if (allFieldParameters.contains(parameter.getName()) == false) {
                PMMLFeatureEntries.Intercept intercept = new PMMLFeatureEntries.Intercept(parameter.getName());
                intercept.addVectorEntry(numFeatures, null);
                numFeatures++;
                featureEntriesMap.add(intercept);
                orderedParameterList.add(parameter.getName());
            }
        }

    }

    private static VectorEntriesPMML createGeneralizedRegressionModelVectorEntries(List<FeatureEntries>
                                                                                           featureEntriesList, String[] orderedParameterList) {
        int numEntries = 0;
        for (FeatureEntries entry : featureEntriesList) {

            numEntries += entry.size();
        }
        return new VectorEntriesPMML.VectorEntriesPMMLGeneralizedRegression(featureEntriesList, numEntries, orderedParameterList);
    }

    private static List<FeatureEntries> convertToFeatureEntries(PMML pmml, int modelNum, TreeMap<String, List<PPCell>> fieldToPPCellMap,
                                                                List<String> orderedParameterList) {
        // for each predictor: get vector entries?
        List<FeatureEntries> featureEntriesList = new ArrayList<>();
        int indexCounter = 0;
        // for each of the fields create the feature entries
        for (String fieldname : fieldToPPCellMap.keySet()) {
            PMMLFeatureEntries featureEntries = getFeatureEntryFromGeneralRegressionModel(pmml, modelNum, fieldname,
                    fieldToPPCellMap.get(fieldname), indexCounter);
            for (PPCell cell : fieldToPPCellMap.get(fieldname)) {
                orderedParameterList.add(cell.getParameterName());
            }
            indexCounter += featureEntries.size();
            featureEntriesList.add(featureEntries);

        }
        return featureEntriesList;
    }

    private static TreeMap<String, List<PPCell>> mapCellsToFields(GeneralRegressionModel grModel) {
        //get all the field names for multinomialLogistic model
        TreeMap<String, List<PPCell>> fieldToPPCellMap = new TreeMap<>();
        for (Predictor predictor : grModel.getFactorList().getPredictors()) {
            fieldToPPCellMap.put(predictor.getName().toString(), new ArrayList<PPCell>());
        }
        for (Predictor predictor : grModel.getCovariateList().getPredictors()) {
            fieldToPPCellMap.put(predictor.getName().toString(), new ArrayList<PPCell>());
        }

        // get all the entries and sort them by field.
        // then create one feature entry per feild and add them to features.
        // also we must keep a list of parameter names here to make sure the model uses the same order!

        for (PPCell ppcell : grModel.getPPMatrix().getPPCells()) {
            fieldToPPCellMap.get(ppcell.getField().toString()).add(ppcell);
        }
        return fieldToPPCellMap;
    }

    static PMMLFeatureEntries getFeatureEntryFromGeneralRegressionModel(PMML model, int modelIndex, String fieldName, List<PPCell> cells, int indexCounter) {
        if (model.getModels().get(modelIndex) instanceof GeneralRegressionModel == false) {
            throw new UnsupportedOperationException("Can only do GeneralRegressionModel so far");
        }
        if (model.getModels().get(modelIndex).getLocalTransformations() != null) {
            throw new UnsupportedOperationException("Local transformations not implemented yet. ");
        }
        List<DerivedField> derivedFields = new ArrayList<>();
        String rawFieldName = getDerivedFields(fieldName, model.getTransformationDictionary(), derivedFields);
        DataField rawField = getRawDataField(model, rawFieldName);

        PMMLFeatureEntries featureEntries;
        if (rawField == null) {
            throw new UnsupportedOperationException("Could not trace back {} to a raw input field. Maybe saomething is not implemented " +
                    "yet or the PMML file is faulty.");
        } else {
            featureEntries = getFieldVector(cells, indexCounter, derivedFields, rawField);
        }
        return featureEntries;

    }

    private static PMMLFeatureEntries getFieldVector(List<PPCell> cells, int indexCounter, List<DerivedField> derivedFields, DataField rawField) {
        PMMLFeatureEntries featureEntries;
        OpType opType;
        if (derivedFields.size() == 0) {
            opType = rawField.getOpType();
        } else {
            opType = derivedFields.get(0).getOpType();
        }

        if (opType.value().equals("continuous")) {
            featureEntries = new PMMLFeatureEntries.ContinousSingleEntryFeatureEntries(rawField, derivedFields.toArray(new
                    DerivedField[derivedFields
                    .size()]));
        } else if (opType.value().equals("categorical")) {
            featureEntries = new PMMLFeatureEntries.SparseCategorical1OfKFeatureEntries(rawField, derivedFields.toArray(new
                    DerivedField[derivedFields
                    .size()]));
        } else {
            throw new UnsupportedOperationException("Only iplemented continuous and categorical variables so far.");
        }

        for (PPCell cell : cells) {
            featureEntries.addVectorEntry(indexCounter, cell);
            indexCounter++;
        }
        return featureEntries;
    }

    private static DataField getRawDataField(PMML model, String rawFieldName) {
        // now find the actual dataField
        DataField rawField = null;
        for (DataField dataField : model.getDataDictionary().getDataFields()) {
            String rawDataFieldName = dataField.getName().getValue();
            if (rawDataFieldName.equals(rawFieldName)) {
                rawField = dataField;
                break;
            }
        }
        return rawField;
    }

    public static String getDerivedFields(String fieldName, TransformationDictionary transformationDictionary, List<DerivedField>
            derivedFields) {
        // trace back all derived fields until we must arrive at an actual data field. This unfortunately means we have to
        // loop over dervived fields as often as we find one..
        DerivedField lastFoundDerivedField;
        String lastFieldName = fieldName;

        do {
            lastFoundDerivedField = null;
            for (DerivedField derivedField : transformationDictionary.getDerivedFields()) {
                if (derivedField.getName().getValue().equals(lastFieldName)) {
                    lastFoundDerivedField = derivedField;
                    derivedFields.add(derivedField);
                    // now get the next fieldname this field references
                    // this is tricky, because this information can be anywhere...
                    lastFieldName = getReferencedFieldName(derivedField);
                    lastFoundDerivedField = derivedField;
                }
            }
        } while (lastFoundDerivedField != null);
        return lastFieldName;
    }

    static private String getReferencedFieldName(DerivedField derivedField) {
        String referencedField = null;
        if (derivedField.getExpression() != null) {
            if (derivedField.getExpression() instanceof Apply) {
                // TODO throw uoe in case the function is not "if missing" - much more to implement!
                for (Expression expression : ((Apply) derivedField.getExpression()).getExpressions()) {
                    if (expression instanceof FieldRef) {
                        referencedField = ((FieldRef) expression).getField().getValue();
                    }
                }
            } else if (derivedField.getExpression() instanceof NormContinuous) {
                referencedField = ((NormContinuous) derivedField.getExpression()).getField().getValue();
            } else {
                throw new UnsupportedOperationException("So far only Apply expression implemented.");
            }
        } else {
            // there is a million ways in which derived fields can reference other fields.
            // need to implement them all!
            throw new UnsupportedOperationException("So far only implemented if function for derived fields.");
        }

        if (referencedField == null) {
            throw new UnsupportedOperationException("could not find raw field name. Maybe this derived field references another derived field? Did not implement that yet.");
        }
        return referencedField;
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

