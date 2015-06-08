package org.elasticsearch.script;

import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.search.lookup.IndexField;
import org.elasticsearch.search.lookup.IndexFieldTerm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Script for predicting class with an svm model
 */
public class SVMModelScript extends AbstractSearchScript {

    SVMModel model = null;
    ArrayList<String> features = null;
    String field = null;
    double[] tfs = null;

    final static public String SCRIPT_NAME = "svm_model";

    /**
     * Factory that is registered in
     * {@link TokenPlugin#onModule(ScriptModule)}
     * method when the plugin is loaded.
     */
    public static class Factory implements NativeScriptFactory {

        /**
         * This method is called for every search on every shard.
         *
         * @param params list of script parameters passed with the query
         * @return new native script
         */
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) throws ScriptException {
            return new SVMModelScript(params);
        }
    }

    /**
     * @param params terms that a used for classification and model parameters. Initialize
     *               naive bayes model here.
     * @throws ScriptException
     */
    private SVMModelScript(Map<String, Object> params) throws ScriptException {
        // get the terms
        features = (ArrayList<String>) params.get("features");
        // get the field
        field = (String) params.get("field");
        ArrayList weightsArrayList = (ArrayList) params.get("weights");
        Number intercept = (Number) params.get("intercept");
        if (field == null || features == null || weightsArrayList == null || intercept == null) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": one of the following parameters missing: field, features, weights, weights, intercept");
        }
        tfs = new double[features.size()];
        double[] weights = new double[weightsArrayList.size()];
        for (int i = 0; i < weightsArrayList.size(); i++) {
            weights[i] = ((Number) weightsArrayList.get(i)).doubleValue();
        }

        model = new SVMModel(Vectors.dense(weights), intercept.doubleValue());
    }

    @Override
    public Object run() {
        try {
            IndexField indexField = this.indexLookup().get(field);
            for (int i = 0; i < features.size(); i++) {
                IndexFieldTerm indexTermField = indexField.get(features.get(i));
                tfs[i] = indexTermField.tf();
            }
            return model.predict(Vectors.dense(tfs));
        } catch (IOException ex) {
            throw new ScriptException("Model prediction failed: ", ex);
        }
    }
}
