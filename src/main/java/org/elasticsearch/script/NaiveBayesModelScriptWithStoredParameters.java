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

package org.elasticsearch.script;

import org.apache.spark.mllib.classification.NaiveBayesModel;
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
 * Script for predicting class with a Naive Bayes model
 */
public class NaiveBayesModelScriptWithStoredParameters extends AbstractSearchScript {

    NaiveBayesModel model = null;
    String field = null;
    double[] tfs = null;
    ArrayList features = new ArrayList();

    final static public String SCRIPT_NAME = "naive_bayes_model_stored_parameters";

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
            return new NaiveBayesModelScriptWithStoredParameters(params, node.client());
        }

        @Override
        public boolean needsScores() {
            return false;
        }
    }

    /**
     * @param params terms that a used for classification and model parameters. Initialize
     *               naive bayes model here.
     * @throws ScriptException
     */
    private NaiveBayesModelScriptWithStoredParameters(Map<String, Object> params, Client client) throws ScriptException {
        GetResponse parametersDoc = SharedMethods.getStoredParameters(params, client);
        field = (String) params.get("field");
        model = SharedMethods.initializeNaiveBayesModel(features, field, parametersDoc);
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
