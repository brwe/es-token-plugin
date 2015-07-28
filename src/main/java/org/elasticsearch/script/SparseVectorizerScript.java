package org.elasticsearch.script;

import org.apache.lucene.index.Fields;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.plugin.TokenPlugin;

import java.io.IOException;
import java.util.*;

/**
 * looks up the frequencies for a terms list and returns as sparse vector of same dimension as input array length
 * this whole class could be a vectorizer: https://github.com/elastic/elasticsearch/issues/10823
 */
public class SparseVectorizerScript extends ModelScriptWithStoredParametersAndSparseVector {

    // the field containing the terms
    String field = null;
    // the terms for which we need the tfs
    ArrayList<String> features = null;

    Map<String, Integer> wordMap;

    final static public String SCRIPT_NAME = "sparse_vector";

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
            return new SparseVectorizerScript(params);
        }
    }

    /**
     * @param params terms that a scored are placed in this parameter. Initialize
     *               them here.
     * @throws ScriptException
     */
    private SparseVectorizerScript(Map<String, Object> params) throws ScriptException {
        params.entrySet();
        // get the terms
        features = (ArrayList<String>) params.get("features");
        // get the field
        field = (String) params.get("field");
        if (field == null || features == null) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": field or features parameter missing!");
        }
        wordMap = new HashMap<>();
        for (int i = 0; i < features.size(); i++) {
            wordMap.put(features.get(i), i);
        }
    }

    @Override
    public Object run() {
        try {
            Fields fields = indexLookup().termVectors();
            if (fields == null) {
                return Collections.emptyMap();
            } else {
                Tuple<int[], double[]> indicesAndValues = getIndicesAndValuesSortedByIndex(fields, field, wordMap);
                Map<String, Object> map = new HashMap<>();
                map.put("indices", indicesAndValues.v1());
                map.put("values", indicesAndValues.v2());
                return map;
            }
        } catch (IOException ex) {
            throw new ScriptException("Could not create sparse vector: ", ex);
        }
    }
}
