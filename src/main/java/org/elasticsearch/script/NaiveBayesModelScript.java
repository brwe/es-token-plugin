package org.elasticsearch.script;

import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.search.lookup.IndexField;
import org.elasticsearch.search.lookup.IndexFieldTerm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Script for predicting class with a Naive Bayes model
 */
public class NaiveBayesModelScript extends AbstractSearchScript {

    NaiveBayesModel model = null;
    ArrayList<String> features = null;
    String field = null;
    double[] tfs = null;

    final static public String SCRIPT_NAME = "naive_bayes_model";

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
            return new NaiveBayesModelScript(params);
        }
    }

    /**
     * @param params terms that a used for classification and model parameters. Initialize
     *               naive bayes model here.
     * @throws ScriptException
     */
    private NaiveBayesModelScript(Map<String, Object> params) throws ScriptException {
        // get the terms
        features = (ArrayList<String>) params.get("features");
        // get the field
        field = (String) params.get("field");
        ArrayList piAsArrayList = (ArrayList) params.get("pi");
        ArrayList labelsAsArrayList = (ArrayList) params.get("labels");
        ArrayList thetasAsArrayList = (ArrayList) params.get("thetas");
        if (field == null || features == null || piAsArrayList == null || labelsAsArrayList == null || thetasAsArrayList == null) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": one of the following parameters missing: field, features, pi, thetas, labels");
        }
        tfs = new double[features.size()];
        double[] pi = new double[piAsArrayList.size()];
        for (int i = 0; i < piAsArrayList.size(); i++) {
            pi[i] = ((Number) piAsArrayList.get(i)).doubleValue();
        }
        double[] labels = new double[labelsAsArrayList.size()];
        for (int i = 0; i < labelsAsArrayList.size(); i++) {
            labels[i] = ((Number) labelsAsArrayList.get(i)).doubleValue();
        }
        double thetas[][] = new double[labels.length][features.size()];
        for (int i = 0; i < thetasAsArrayList.size(); i++) {
            ArrayList thetaRow = (ArrayList) thetasAsArrayList.get(i);
            for (int j = 0; j < thetaRow.size(); j++) {
                thetas[i][j] = ((Number) thetaRow.get(j)).doubleValue();
            }
        }
        model = new NaiveBayesModel(labels, pi, thetas);
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
