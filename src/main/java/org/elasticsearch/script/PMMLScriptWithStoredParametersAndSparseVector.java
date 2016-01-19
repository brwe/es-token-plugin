package org.elasticsearch.script;


import org.dmg.pmml.Model;
import org.dmg.pmml.NumericPredictor;
import org.dmg.pmml.PMML;
import org.dmg.pmml.RegressionModel;
import org.dmg.pmml.RegressionTable;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.node.Node;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.transform.Source;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
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

public class PMMLScriptWithStoredParametersAndSparseVector extends AbstractSearchScript {

    final static public String SCRIPT_NAME = "pmml_model_stored_parameters_sparse_vectors";
    EsModelEvaluator model = null;
    String field = null;
    ArrayList<String> features = new ArrayList();
    Map<String, Integer> wordMap;
    private boolean fieldDataFields;

    /**
     * Factory that is registered in
     * {@link org.elasticsearch.plugin.TokenPlugin#onModule(ScriptModule)}
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
            try {
                return new PMMLScriptWithStoredParametersAndSparseVector(params, node.client());
            } catch (IOException e) {
                throw new ScriptException("pmml prediction failed", e);
            } catch (SAXException e) {
                throw new ScriptException("pmml prediction failed", e);
            } catch (JAXBException e) {
                throw new ScriptException("pmml prediction failed", e);
            }
        }
    }

    /**
     * @param params terms that a used for classification and model parameters. Initialize model here.
     * @throws ScriptException
     */
    private PMMLScriptWithStoredParametersAndSparseVector(Map<String, Object> params, Client client) throws ScriptException, IOException, SAXException, JAXBException {
        GetResponse getResponse = SharedMethods.getStoredParameters(params, client);
        PMML pmml;

        try (InputStream is = new ByteArrayInputStream(getResponse.getSourceAsMap().get("pmml").toString().getBytes(Charset.defaultCharset()))) {
            Source transformedSource = ImportFilter.apply(new InputSource(is));
            pmml = JAXBUtil.unmarshalPMML(transformedSource);
        }

        Model model = pmml.getModels().get(0);
        if(model.getModelName().equals("logistic regression")) {
            initLogisticRegression(model);
        } else {
            throw new UnsupportedOperationException("We only implemented logistic regression so far but your model is of type " + model.getModelName());
        }
        field = (String) params.get("field");
        fieldDataFields = (params.get("fieldDataFields") == null) ? fieldDataFields : (Boolean) params.get("fieldDataFields");
        features.addAll((ArrayList) getResponse.getSource().get("features"));
        wordMap = new HashMap<>();
        SharedMethods.fillWordIndexMap(features, wordMap);
    }

    private void initLogisticRegression(Model pmmlModel) {
        RegressionModel regressionModel = (RegressionModel) pmmlModel;
        RegressionTable regressionTable = regressionModel.getRegressionTables().get(0);
        List<NumericPredictor> numericPredictors = regressionTable.getNumericPredictors();
        double[] coefficients = new double[numericPredictors.size()];
        int i = 0;
        for (NumericPredictor numericPredictor : numericPredictors) {
            coefficients[i] = numericPredictor.getCoefficient();
            i++;
        }
        model = new EsLogisticRegressionModel(coefficients, regressionTable.getIntercept(),new Tuple<String, String>(((RegressionModel) pmmlModel).getRegressionTables().get(0).getTargetCategory(), ((RegressionModel) pmmlModel).getRegressionTables().get(1).getTargetCategory()));

    }

    @Override
    public Object run() {
        /** here be the vectorizer **/
        Tuple<int[], double[]> fieldNamesAndValues;
        if (fieldDataFields == false) {
            throw new UnsupportedOperationException("term vectors not implemented for PMML");
        } else {
            ScriptDocValues<String> docValues = docFieldStrings(field);
            fieldNamesAndValues = SharedMethods.getIndicesAndValuesFromFielddataFields(wordMap, docValues);
        }
        /** until here **/
        return model.evaluate(fieldNamesAndValues);
    }
}
