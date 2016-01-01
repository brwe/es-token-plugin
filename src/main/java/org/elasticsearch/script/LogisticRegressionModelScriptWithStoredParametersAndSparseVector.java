package org.elasticsearch.script;

import org.apache.lucene.index.Fields;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Script for predicting a class based on a text field with an SVM model. This needs the parameters "weights" and "intercept"
 * to be stored in a document elasticsearch together with the relevant features (words).
 * This script expects that term vectors are stored. You can use the same thing without term
 * vectors with SVMModelScriptWithStoredParameters but that is very slow.
 * <p/>
 * Say for example the parameters are stored as
 * <pre>
 *     @code
 *     {
 *      "_index": "model",
 *      "_type": "params",
 *      "_id": "svm_model_params",
 *      "_version": 2,
 *      "found": true,
 *      "_source": {
 *          "features": [ "bad","boring","both","life","perfect","performances","plot","stupid","world","worst"],
 *          "weights": "[-0.07491787895405054,-0.05231695457685094,0.03431992220241421,0.06571009494852478,0.030971637109495756,0.04603892002762882,-0.04478331311778441,-0.035575529112258635,0.05189841894023615,-0.056083775306384205]",
 *          "intercept": 0
 *      }
 *     }
 *
 * </pre>
 * <p/>
 * Then a request to classify documents would look like this:
 * <p/>
 * <p/>
 * <p/>
 * <p/>
 * <pre>
 *  @code
 * GET twitter/tweets/_search
 * {
 *  "script_fields": {
 *      "predicted_label": {
 *          "script": "svm_model_stored_parameters_sparse_vectors",
 *          "lang": "native",
 *          "params": {
 *              "field": "message",
 *              "index": "model",
 *              "type": "params",
 *              "id": "svm_model_params"
 *          }
 *      }
 *  }
 * }
 * </pre>
 */

public class LogisticRegressionModelScriptWithStoredParametersAndSparseVector extends AbstractSearchScript {

    final static public String SCRIPT_NAME = "lr_model_stored_parameters_sparse_vectors";
    ClassificationModel model = null;
    String field = null;
    ArrayList features = new ArrayList();
    Map<String, Integer> wordMap;
    List<Integer> indices = new ArrayList<>();
    List<Integer> values = new ArrayList<>();
    private boolean fieldDataFields;

    /**
     * Factory that is registered in
     * {@link org.elasticsearch.plugin.TokenPlugin#onModule(org.elasticsearch.script.ScriptModule)}
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
            return new LogisticRegressionModelScriptWithStoredParametersAndSparseVector(params, node.client());
        }
    }

    /**
     * @param params terms that a used for classification and model parameters. Initialize model here.
     * @throws org.elasticsearch.script.ScriptException
     */
    private LogisticRegressionModelScriptWithStoredParametersAndSparseVector(Map<String, Object> params, Client client) throws ScriptException {
        GetResponse getResponse = SharedMethods.getStoredParameters(params, client);
        field = (String) params.get("field");
        fieldDataFields = (params.get("fieldDataFields") == null) ? fieldDataFields : (Boolean) params.get("fieldDataFields");
        model = SharedMethods.initializeLRModel(features, field, getResponse);
        wordMap = new HashMap<>();
        SharedMethods.fillWordIndexMap(features, wordMap);
    }

    @Override
    public Object run() {
        try {
            /** here be the vectorizer **/
            Tuple<int[], double[]> indicesAndValues;
            if (fieldDataFields == false) {
                Fields fields = indexLookup().termVectors();
                if (fields == null) {
                    return -1;
                }
                indicesAndValues = SharedMethods.getIndicesAndValuesFromTermVectors(indices, values, fields, field, wordMap);

            } else {
                ScriptDocValues<String> docValues = docFieldStrings(field);
                indicesAndValues = SharedMethods.getIndicesAndValuesFromFielddataFields(wordMap, docValues);
            }
            /** until here **/
            return model.predict(Vectors.sparse(features.size(), indicesAndValues.v1(), indicesAndValues.v2()));
        } catch (IOException ex) {
            throw new ScriptException("Model prediction failed: ", ex);
        }
    }

}
