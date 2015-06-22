package org.elasticsearch.script;

import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugin.TokenPlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Script for predicting class with a Naive Bayes model
 */
public class SVMModelScriptWithStoredParametersAndSparseVector extends ModelScriptWithStoredParametersAndSparseVector {

    final static public String SCRIPT_NAME = "svm_model_stored_parameters_sparse_vectors";

    /**
     * Factory that is registered in
     * {@link TokenPlugin#onModule(ScriptModule)}
     * method when the plugin is loaded.
     */
    public static class Factory implements NativeScriptFactory {

        final Node node;

        @Inject
        public Factory(Node node) {
            // Node is not fully initialized here
            // All we can do is save a reference to it for future use
            this.node = node;
        }

        /**
         * This method is called for every search on every shard.
         *
         * @param params list of script parameters passed with the query
         * @return new native script
         */
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) throws ScriptException {
            return new SVMModelScriptWithStoredParametersAndSparseVector(params, node.client());
        }
    }

    /**
     * @param params terms that a used for classification and model parameters. Initialize
     *               naive bayes model here.
     * @throws ScriptException
     */
    private SVMModelScriptWithStoredParametersAndSparseVector(Map<String, Object> params, Client client) throws ScriptException {
        // get the terms

        String index = (String) params.get("index");
        if (index == null) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": parameter \"index\" missing");
        }
        String type = (String) params.get("type");
        if (index == null) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": parameter \"type\" missing");
        }
        String id = (String) params.get("id");
        if (index == null) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": parameter \"id\" missing");
        }

        // get the parameters from somewhere else
        GetResponse getResponse = client.prepareGet(index, type, id).get();
        if (getResponse.isExists() == false) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": document " + index + "/" + type + "/" + id);
        }

        // get the field
        field = (String) params.get("field");
        ArrayList weightsArrayList = (ArrayList) getResponse.getSource().get("weights");
        double[] weights = new double[weightsArrayList.size()];
        for (int i = 0; i < weightsArrayList.size(); i++) {
            weights[i] = ((Number) weightsArrayList.get(i)).doubleValue();
        }
        Number intercept = (Number) getResponse.getSource().get("intercept");
        features = (ArrayList) getResponse.getSource().get("features");
        if (field == null || features == null || weightsArrayList == null || intercept == null) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": one of the following parameters missing: field, features, weights, weights, intercept");
        }
        model = new SVMModel(Vectors.dense(weights), intercept.doubleValue());
        wordMap = new HashMap<>();
        for (int i = 0; i < features.size(); i++) {
            wordMap.put((String) features.get(i), i);
        }
    }

}
