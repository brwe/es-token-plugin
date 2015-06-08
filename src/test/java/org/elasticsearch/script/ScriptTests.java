package org.elasticsearch.script;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

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
        parameters.put("features", new String[]{"the", "quick", "fox"});
        parameters.put("field", "text");
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("vector", "native", "vector", parameters).get();
        double[] vector = (double[]) (searchResponse.getHits().getAt(0).field("vector").values().get(0));
        assertThat(vector.length, equalTo(3));
        assertThat(vector[0], equalTo(1.0));
        assertThat(vector[1], equalTo(2.0));
        assertThat(vector[2], equalTo(1.0));

    }

    @Test
    // only just checks that nothing crashes
    public void testModelScripts() {
        client().prepareIndex().setId("1").setIndex("index").setType("type").setSource("text", "the quick brown fox is quick").get();
        ensureGreen("index");
        refresh();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("features", new String[]{"the", "quick", "fox"});
        parameters.put("pi", new double[]{1, 2});
        parameters.put("thetas", new double[][]{{1, 2, 3}, {3, 2, 1}});
        parameters.put("labels", new double[]{0, 1});
        parameters.put("field", "text");
        SearchResponse searchResponse = client().prepareSearch("index").addScriptField("nb", "native", "naive_bayes_model", parameters).get();
        double label = (Double) (searchResponse.getHits().getAt(0).field("nb").values().get(0));
        parameters.clear();
        parameters.put("features", new String[]{"the", "quick", "fox"});
        parameters.put("weights", new double[]{0, 1, 3});
        parameters.put("intercept", 1);
        parameters.put("field", "text");
        searchResponse = client().prepareSearch("index").addScriptField("svm", "native", "svm_model", parameters).get();
        label = (Double) (searchResponse.getHits().getAt(0).field("svm").values().get(0));


    }
}
