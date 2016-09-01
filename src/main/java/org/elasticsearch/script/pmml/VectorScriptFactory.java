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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.ml.modelinput.DataSource;
import org.elasticsearch.ml.modelinput.EsDataSource;
import org.elasticsearch.ml.modelinput.VectorRangesToVector;
import org.elasticsearch.ml.modelinput.VectorRangesToVectorJSON;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafIndexLookup;

import java.util.Map;

/**
 * Can read json def and return sparse vectors with tfs.
 */
public class VectorScriptFactory implements NativeScriptFactory {

    public static final String NAME = "doc_to_vector";

    public VectorScriptFactory() {

    }

    @Override
    public ExecutableScript newScript(@Nullable Map<String, Object> params) {
        if (params == null || params.containsKey("spec") == false) {
            throw new IllegalArgumentException("the spec parameter is required");
        }
        Map<String, Object> spec = XContentMapValues.nodeMapValue(params.get("spec"), "spec");
        // TODO: Add caching mechanism
        VectorRangesToVector features = new VectorRangesToVectorJSON(spec);
        return new VectorizerScript(features);
    }

    @Override
    public boolean needsScores() {
        // TODO: can we reliably know if a vectorizer script does not make use of _score
        return false;
    }

    @Override
    public String getName() {
        return NAME;
    }

    public static class VectorizerScript extends AbstractSearchScript {

        private final VectorRangesToVector features;

        private DataSource dataSource;

        /**
         * Factory that is registered in
         * {@link TokenPlugin#onModule(org.elasticsearch.script.ScriptModule)}
         * method when the plugin is loaded.
         */

        private VectorizerScript(VectorRangesToVector features) {
            this.features = features;
            dataSource = new EsDataSource() {
                @Override
                protected LeafDocLookup getDocLookup() {
                    return doc();
                }

                @Override
                protected LeafIndexLookup getLeafIndexLookup() {
                    return indexLookup();
                }
            };
        }

        @Override
        public Object run() {
            return features.convert(dataSource).getAsMap();
        }
    }


}

