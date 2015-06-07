package org.elasticsearch.script;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.search.lookup.IndexField;
import org.elasticsearch.search.lookup.IndexFieldTerm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * looks up the frequencies for a terms list and returns as vector of same dimension as input array length
 */
public class VectorizerScript extends AbstractSearchScript {

    // the field containing the terms
    String field = null;
    // the terms for which we need the tfs
    ArrayList<String> features = null;

    final static public String SCRIPT_NAME = "vector";

    /**
     * Factory that is registered in
     * {@link org.elasticsearch.plugin.mapper.token.AnalyzedTextPlugin#onModule(org.elasticsearch.script.ScriptModule)}
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
