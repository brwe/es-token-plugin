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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugin.TokenPlugin;
import org.elasticsearch.script.*;
import org.elasticsearch.script.modelinput.FieldsToVector;
import org.elasticsearch.script.modelinput.FieldsToVectorJSON;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

/**
 * Provides the infrastructure for Groovy as a scripting language for Elasticsearch
 */
public class PMMLVectorScriptEngineService extends AbstractComponent implements ScriptEngineService {

    public static final String NAME = "doc_to_vector";

    @Inject
    public PMMLVectorScriptEngineService(Settings settings) {
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
        throw new UnsupportedOperationException("vectorizer script not supported in this context!");
    }

    public static class Factory {
        FieldsToVector features = null;

        public Factory(String spec) {
            Map<String, Object> parsedSource = null;
            try {
                XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(spec);
                parsedSource = parser.mapOrdered();
            } catch (IOException e) {
                throw new ScriptException("vector script failed", e);
            }
            features = new FieldsToVectorJSON(parsedSource);
        }

        public VectorizerScript newScript(LeafSearchLookup lookup) {
            return new VectorizerScript(features, lookup);
        }
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public SearchScript search(final CompiledScript compiledScript, final SearchLookup lookup, @Nullable final Map<String, Object> vars) {
        return new SearchScript() {

            @Override
            public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
                final LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);
                VectorizerScript scriptObject = ((Factory) compiledScript.compiled()).newScript(leafLookup);
                return scriptObject;
            }

            @Override
            public boolean needsScores() {
                // TODO: can we reliably know if a vectorizer script does not make use of _score
                return false;
            }
        };
    }

    public static class VectorizerScript implements LeafSearchScript {

        private final FieldsToVector features;
        private LeafSearchLookup lookup;

        /**
         * Factory that is registered in
         * {@link TokenPlugin#onModule(org.elasticsearch.script.ScriptModule)}
         * method when the plugin is loaded.
         */

        /**
         * @throws ScriptException
         */
        private VectorizerScript(FieldsToVector features, LeafSearchLookup lookup) throws ScriptException {
            this.lookup = lookup;
            this.features = features;

        }

        @Override
        public void setNextVar(String s, Object o) {

        }

        @Override
        public Object run() {
            return features.vector(lookup.doc(), lookup.fields(), lookup.indexLookup(), lookup.source());
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
            throw new UnsupportedOperationException("vectorizer script not supported in this context!");
        }

        @Override
        public long runAsLong() {
            throw new UnsupportedOperationException("vectorizer script not supported in this context!");
        }

        @Override
        public double runAsDouble() {
            throw new UnsupportedOperationException("vectorizer script not supported in this context!");
        }

        @Override
        public void setScorer(Scorer scorer) {

        }
    }


}

