package org.elasticsearch.script;

import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.search.lookup.IndexField;
import org.elasticsearch.search.lookup.IndexFieldTerm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Script for predicting class with a SVM model
 */
public class SVMModelScriptWithStoredParameters extends AbstractSearchScript {

    SVMModel model = null;
    String field = null;
    double[] tfs = null;
    ArrayList features = new ArrayList();

    final static public String SCRIPT_NAME = "svm_model_stored_parameters";

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
            return new SVMModelScriptWithStoredParameters(params, node.client());
        }
    }

    /**
     * @param params index, type and id of document containing the parameters. also fieldname.
     * @throws ScriptException
     */
    private SVMModelScriptWithStoredParameters(Map<String, Object> params, Client client) throws ScriptException {
        GetResponse getResponse = SharedMethods.getStoredParameters(params, client);
        field = (String) params.get("field");
        model = SharedMethods.initializeSVMModel(features, field, getResponse);
        tfs = new double[features.size()];
    }

    @Override
    public Object run() {
        try {
            /** here be the vectorizer **/
            IndexField indexField = this.indexLookup().get(field);
            for (int i = 0; i < features.size(); i++) {
                IndexFieldTerm indexTermField = indexField.get(features.get(i));
                tfs[i] = indexTermField.tf();
            }
            /** until here **/
            return model.predict(Vectors.dense(tfs));
        } catch (IOException ex) {
            throw new ScriptException("Model prediction failed: ", ex);
        }
    }
}
