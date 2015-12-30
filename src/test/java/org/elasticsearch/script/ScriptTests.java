package org.elasticsearch.script;

import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.linalg.DenseVector;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.jpmml.evaluator.*;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.jpmml.model.visitors.LocatorTransformer;
import org.junit.Test;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.transform.Source;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class ScriptTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .build();
    }

    @Test
    public void testVectorScript() {
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        ensureGreen("index");
        refresh();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("features", new String[]{"fox", "quick", "the"});
        parameters.put("field", "text");
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("vector", "native", "vector", parameters).get();
        double[] vector = (double[]) (searchResponse.getHits().getAt(0).field("vector").values().get(0));
        assertThat(vector.length, equalTo(3));
        assertThat(vector[0], equalTo(1.0));
        assertThat(vector[1], equalTo(2.0));
        assertThat(vector[2], equalTo(1.0));

    }

    @Test
    public void testSparseVectorScript() throws IOException {
        createIndexWithTermVectors();
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown the fox is the quick").get();
        ensureGreen("index");
        refresh();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("features", new String[]{"foo", "fox", "quick", "the"});
        parameters.put("field", "text");
        boolean fieldDataFields = randomBoolean();
        parameters.put("fieldDataFields", fieldDataFields);
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("vector", "native", "sparse_vector", parameters).get();
        assertSearchResponse(searchResponse);
        int[] vector = (int[]) ((Map<String, Object>) (searchResponse.getHits().getAt(0).field("vector").values().get(0))).get("indices");
        assertThat(vector.length, equalTo(3));
        assertThat(vector[0], equalTo(1));
        assertThat(vector[1], equalTo(2));
        assertThat(vector[2], equalTo(3));
        double[] value = (double[]) ((Map<String, Object>) (searchResponse.getHits().getAt(0).field("vector").values().get(0))).get("values");
        assertThat(value.length, equalTo(3));
        if (fieldDataFields) {
            assertThat(value[0], equalTo(1.0));
            assertThat(value[1], equalTo(1.0));
            assertThat(value[2], equalTo(1.0));
        } else {
            assertThat(value[0], equalTo(1.0));
            assertThat(value[1], equalTo(2.0));
            assertThat(value[2], equalTo(3.0));
        }
    }

    @Test
    // only just checks that nothing crashes
    public void testModelScriptsWithStoredParams() throws IOException {
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        ensureGreen("index");
        client().prepareIndex("model", "params", "test_params").setSource(
                jsonBuilder().startObject()
                        .field("pi", new double[]{1, 2})
                        .field("thetas", new double[][]{{1, 2, 3}, {3, 2, 1}})
                        .field("labels", new double[]{0, 1})
                        .field("features", new String[]{"fox", "quick", "the"})
                        .endObject()
        ).get();
        refresh();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("index", "model");
        parameters.put("type", "params");
        parameters.put("id", "test_params");
        parameters.put("field", "text");
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("nb", "native", NaiveBayesModelScriptWithStoredParameters.SCRIPT_NAME, parameters).get();
        assertSearchResponse(searchResponse);
        double label = (Double) (searchResponse.getHits().getAt(0).field("nb").values().get(0));
        client().prepareIndex("model", "params", "test_params").setSource(
                jsonBuilder().startObject()
                        .field("weights", new double[]{1, 2, 3})
                        .field("intercept", 0.5)
                        .field("features", new String[]{"fox", "quick", "the"})
                        .endObject()
        ).get();
        refresh();
        searchResponse = client().prepareSearch("index").addScriptField("svm", "native", SVMModelScriptWithStoredParameters.SCRIPT_NAME, parameters).get();
        label = (Double) (searchResponse.getHits().getAt(0).field("svm").values().get(0));
    }

    @Test
    // only just checks that nothing crashes
    public void testModelScriptsWithStoredParamsAndSparseVectors() throws IOException {
        createIndexWithTermVectors();
        ensureGreen();
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        ensureGreen("index");
        client().prepareIndex("model", "params", "test_params").setSource(
                jsonBuilder().startObject()
                        .field("pi", new double[]{1, 2})
                        .field("thetas", new double[][]{{1, 2, 3}, {3, 2, 1}})
                        .field("labels", new double[]{0, 1})
                        .field("features", new String[]{"fox", "quick", "the"})
                        .endObject()
        ).get();
        refresh();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("index", "model");
        parameters.put("type", "params");
        parameters.put("id", "test_params");
        parameters.put("field", "text");
        if (randomBoolean()) {
            parameters.put("fieldDataFields", true);
        }
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("nb", "native", NaiveBayesModelScriptWithStoredParametersAndSparseVector.SCRIPT_NAME, parameters).get();
        assertSearchResponse(searchResponse);
        double label = (Double) (searchResponse.getHits().getAt(0).field("nb").values().get(0));
        client().prepareIndex("model", "params", "test_params").setSource(
                jsonBuilder().startObject()
                        .field("weights", new double[]{1, 2, 3})
                        .field("intercept", 0.5)
                        .field("features", new String[]{"fox", "quick", "the"})
                        .endObject()
        ).get();
        refresh();
        searchResponse = client().prepareSearch("index").addScriptField("svm", "native", SVMModelScriptWithStoredParametersAndSparseVector.SCRIPT_NAME, parameters).get();
        assertSearchResponse(searchResponse);
        label = (Double) (searchResponse.getHits().getAt(0).field("svm").values().get(0));
    }

    @Test
    // only just checks that nothing crashes
    public void testNaiveBayesUpdateScript() throws IOException {

        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        refresh();
        client().prepareIndex("model", "params", "test_params").setSource(
                jsonBuilder().startObject()
                        .field("pi", new double[]{1, 2})
                        .field("thetas", new double[][]{{1, 2, 3}, {3, 2, 1}})
                        .field("labels", new double[]{0, 1})
                        .field("features", new String[]{"fox", "quick", "the"})
                        .endObject()
        ).get();
        refresh();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("index", "model");
        parameters.put("type", "params");
        parameters.put("id", "test_params");
        parameters.put("field", "text");
        if (randomBoolean()) {
            parameters.put("fieldDataFields", true);
        }
        client().prepareUpdate().setId("1").setIndex("index").setType("type")
                .setScriptParams(parameters)
                .setScript(NaiveBayesUpdateScript.SCRIPT_NAME, ScriptService.ScriptType.INLINE)
                .setScriptLang("native").get();

        GetResponse getResponse = client().prepareGet().setId("1").setIndex("index").setType("type").get();
        assertNotNull(getResponse.getSource().get("label"));
    }

    @Test
    // only just checks that nothing crashes
    public void testPMMLBasic() throws IOException, JAXBException, SAXException {
        for (int i = 0; i< 1000; i++) {

            double[] modelParams = new double[]{randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100)};
            SVMModel svmm = new SVMModel(new DenseVector(modelParams), 0.1);
            String pmmlString = "<PMML xmlns=\"http://www.dmg.org/PMML-4_2\">\n" +
                    "    <Header description=\"linear SVM\">\n" +
                    "        <Application name=\"Apache Spark MLlib\" version=\"1.5.2\"/>\n" +
                    "        <Timestamp>2015-12-30T13:51:42</Timestamp>\n" +
                    "    </Header>\n" +
                    "    <DataDictionary numberOfFields=\"4\">\n" +
                    "        <DataField name=\"field_0\" optype=\"continuous\" dataType=\"double\"/>\n" +
                    "        <DataField name=\"field_1\" optype=\"continuous\" dataType=\"double\"/>\n" +
                    "        <DataField name=\"field_2\" optype=\"continuous\" dataType=\"double\"/>\n" +
                    "        <DataField name=\"target\" optype=\"categorical\" dataType=\"string\"/>\n" +
                    "    </DataDictionary>\n" +
                    "    <RegressionModel modelName=\"linear SVM\" functionName=\"classification\" normalizationMethod=\"none\">\n" +
                    "        <MiningSchema>\n" +
                    "            <MiningField name=\"field_0\" usageType=\"active\"/>\n" +
                    "            <MiningField name=\"field_1\" usageType=\"active\"/>\n" +
                    "            <MiningField name=\"field_2\" usageType=\"active\"/>\n" +
                    "            <MiningField name=\"target\" usageType=\"target\"/>\n" +
                    "        </MiningSchema>\n" +
                    "        <RegressionTable intercept=\"0.1\" targetCategory=\"1\">\n" +
                    "            <NumericPredictor name=\"field_0\" coefficient=\"" + modelParams[0] + "\"/>\n" +
                    "            <NumericPredictor name=\"field_1\" coefficient=\"" + modelParams[1] + "\"/>\n" +
                    "            <NumericPredictor name=\"field_2\" coefficient=\"" + modelParams[2] + "\"/>\n" +
                    "        </RegressionTable>\n" +
                    "        <RegressionTable intercept=\"0.0\" targetCategory=\"0\"/>\n" +
                    "    </RegressionModel>\n" +
                    "</PMML>";
            client().prepareIndex("test", "type", "1").setSource(jsonBuilder().startObject().field("pmml", pmmlString).endObject()).get();
            GetResponse doc = client().prepareGet("test", "type", "1").get();
            PMML pmml;
            try (InputStream is = new ByteArrayInputStream(doc.getSourceAsMap().get("pmml").toString().getBytes(Charset.defaultCharset()))) {
                Source transformedSource = ImportFilter.apply(new InputSource(is));
                pmml = JAXBUtil.unmarshalPMML(transformedSource);
            }
            ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
            Evaluator evaluator = modelEvaluatorFactory.newModelManager(pmml);
            evaluator.verify();
            Map<FieldName, Object> params = new HashMap<>();
            double[] vals = new double[]{randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100), randomFloat() * randomIntBetween(-100, +100)};
            params.put(new FieldName("field_0"), new Double(vals[0]));
            params.put(new FieldName("field_1"), new Double(vals[1]));
            params.put(new FieldName("field_2"), new Double(vals[2]));
            Map<FieldName, ?> result = evaluator.evaluate(params);
            double mllibResult = svmm.predict(new DenseVector(vals));
            String pmmlResult = (String) ((ProbabilityDistribution) result.get(new FieldName("target"))).getResult();
            assertThat(mllibResult, equalTo(Double.parseDouble(pmmlResult)));
        }
    }

    void createIndexWithTermVectors() throws IOException {
        XContentBuilder mapping = jsonBuilder();
        mapping.startObject()
                .startObject("type")
                .startObject("properties")
                .startObject("text")
                .field("type", "string")
                .field("term_vector", "yes")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        client().admin().indices().prepareCreate("index").addMapping("type", mapping).get();
    }
}
