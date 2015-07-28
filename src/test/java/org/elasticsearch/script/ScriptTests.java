package org.elasticsearch.script;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
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
        parameters.put("features", new String[]{"fox", "quick",  "the"});
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
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("vector", "native", "sparse_vector", parameters).get();
        assertSearchResponse(searchResponse);
        int[] vector = (int[]) ((Map<String, Object>) (searchResponse.getHits().getAt(0).field("vector").values().get(0))).get("indices");
        assertThat(vector.length, equalTo(3));
        assertThat(vector[0], equalTo(1));
        assertThat(vector[1], equalTo(2));
        assertThat(vector[2], equalTo(3));
        double[] value = (double[]) ((Map<String, Object>) (searchResponse.getHits().getAt(0).field("vector").values().get(0))).get("values");
        assertThat(value.length, equalTo(3));
        assertThat(value[0], equalTo(1.0));
        assertThat(value[1], equalTo(2.0));
        assertThat(value[2], equalTo(3.0));
    }

    @Test
    // only just checks that nothing crashes
    public void testModelScripts() {
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        ensureGreen("index");
        refresh();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("features", new String[]{"fox", "quick",  "the"});
        parameters.put("pi", new double[]{1, 2});
        parameters.put("thetas", new double[][]{{1, 2, 3}, {3, 2, 1}});
        parameters.put("labels", new double[]{0, 1});
        parameters.put("field", "text");
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("nb", "native", "naive_bayes_model", parameters).get();
        double label = (Double) (searchResponse.getHits().getAt(0).field("nb").values().get(0));
        parameters.clear();
        parameters.put("features", new String[]{"fox", "quick",  "the"});
        parameters.put("weights", new double[]{0, 1, 3});
        parameters.put("intercept", 1);
        parameters.put("field", "text");
        searchResponse = client().prepareSearch("index").addScriptField("svm", "native", "svm_model", parameters).get();
        label = (Double) (searchResponse.getHits().getAt(0).field("svm").values().get(0));
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
                        .field("features", new String[]{"fox", "quick",  "the"})
                        .endObject()
        ).get();
        refresh();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("index", "model");
        parameters.put("type", "params");
        parameters.put("id", "test_params");
        parameters.put("field", "text");
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("nb", "native", NaiveBayesModelScriptWithStoredParameters.SCRIPT_NAME, parameters).get();
        double label = (Double) (searchResponse.getHits().getAt(0).field("nb").values().get(0));
        client().prepareIndex("model", "params", "test_params").setSource(
                jsonBuilder().startObject()
                        .field("weights", new double[]{1, 2, 3})
                        .field("intercept", 0.5)
                        .field("features", new String[]{"fox", "quick",  "the"})
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
                        .field("features", new String[]{"fox", "quick",  "the"})
                        .endObject()
        ).get();
        refresh();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("index", "model");
        parameters.put("type", "params");
        parameters.put("id", "test_params");
        parameters.put("field", "text");
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("nb", "native", NaiveBayesModelScriptWithStoredParametersAndSparseVector.SCRIPT_NAME, parameters).get();
        double label = (Double) (searchResponse.getHits().getAt(0).field("nb").values().get(0));
        client().prepareIndex("model", "params", "test_params").setSource(
                jsonBuilder().startObject()
                        .field("weights", new double[]{1, 2, 3})
                        .field("intercept", 0.5)
                        .field("features", new String[]{"fox", "quick",  "the"})
                        .endObject()
        ).get();
        refresh();
        searchResponse = client().prepareSearch("index").addScriptField("svm", "native", SVMModelScriptWithStoredParametersAndSparseVector.SCRIPT_NAME, parameters).get();
        assertSearchResponse(searchResponse);
        label = (Double) (searchResponse.getHits().getAt(0).field("svm").values().get(0));
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
