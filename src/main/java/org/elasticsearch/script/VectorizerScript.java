
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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.search.lookup.IndexField;
import org.elasticsearch.search.lookup.IndexFieldTerm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * looks up the frequencies for a terms list and returns as vector of same dimension as input array length
 * this whole class could be a vectorizer: https://github.com/elastic/elasticsearch/issues/10823
 */
public class VectorizerScript extends AbstractSearchScript {

    // the field containing the terms
    String field = null;
    // the terms for which we need the tfs
    ArrayList<String> features = null;

    final static public String SCRIPT_NAME = "vector";

    /**
     * Factory that is registered in
     * {@link TokenPlugin#onModule(org.elasticsearch.script.ScriptModule)}
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
            return new VectorizerScript(params);
        }

        @Override
        public boolean needsScores() {
            return false;
        }
    }

    /**
     * @param params terms that a scored are placed in this parameter. Initialize
     *               them here.
     * @throws ScriptException
     */
    private VectorizerScript(Map<String, Object> params) throws ScriptException {
        params.entrySet();
        // get the terms
        features = (ArrayList<String>) params.get("features");
        // get the field
        field = (String) params.get("field");
        if (field == null || features == null) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": field or features parameter missing!");
        }
    }

    @Override
    public Object run() {
        double[] tfs = new double[features.size()];
        try {
            IndexField indexField = this.indexLookup().get(field);
            for (int i = 0; i < features.size(); i++) {
                IndexFieldTerm indexTermField = indexField.get(features.get(i));
                tfs[i] = indexTermField.tf();
            }
            return tfs;
        } catch (IOException ex) {
            throw new ScriptException("Could not get tf vector: ", ex);
        }
    }
}
