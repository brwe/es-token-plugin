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

import org.dmg.pmml.PMML;
import org.elasticsearch.script.modelinput.DataSource;
import org.elasticsearch.script.modelinput.VectorRangesToVectorPMML;
import org.elasticsearch.script.modelinput.MapModelInput;
import org.elasticsearch.script.modelinput.ModelAndModelInputEvaluator;
import org.elasticsearch.script.pmml.ModelFactories;
import org.elasticsearch.script.pmml.ProcessPMMLHelper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;

public class VectorizerPMMLSingleNodeTests extends ESTestCase {


    private DataSource createTestDataSource(String[] work, String education, Integer age) throws IOException {
        Map<String, List<Object>> data = new HashMap<>();
        if (work != null) {
            Arrays.sort(work);
            data.put("work", Arrays.asList((Object[])work));
        }
        if (education != null) {
            data.put("education", Collections.singletonList(education));
        }
        if (age != null) {
            data.put("age", Collections.singletonList(age));
        }
        return new MockDataSource(data);
    }

    @SuppressWarnings("unchecked")
    public void testGLMOnActualLookup() throws Exception {
        ModelFactories parser = ModelFactories.createDefaultModelFactories();
        DataSource dataSource = createTestDataSource(new String[]{"Self-emp-inc"}, null, 60);
        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/fake_lr_model_with_missing.xml");
        PMML pmml = ProcessPMMLHelper.parsePmml(pmmlString);
        ModelAndModelInputEvaluator<MapModelInput> fieldsToVectorAndModel = parser.buildFromPMML(pmml, 0);
        VectorRangesToVectorPMML vectorEntries = (VectorRangesToVectorPMML) fieldsToVectorAndModel.getVectorRangesToVector();
        Map<String, Object> vector = vectorEntries.convert(dataSource).getAsMap();
        assertThat(((double[]) vector.get("values")).length, equalTo(3));
        assertThat(((int[]) vector.get("indices")).length, equalTo(3));
        assertArrayEquals((int[]) vector.get("indices"), new int[]{0, 2, 5});
        assertArrayEquals((double[]) vector.get("values"), new double[]{1.1724330344107299, 1.0, 1.0}, 1.e-7);

        // test missing values
        dataSource = createTestDataSource(new String[]{"Self-emp-inc"}, null, null);
        vector = vectorEntries.convert(dataSource).getAsMap();
        assertThat(((double[]) vector.get("values")).length, equalTo(3));
        assertThat(((int[]) vector.get("indices")).length, equalTo(3));
        assertArrayEquals((int[]) vector.get("indices"), new int[]{0, 2, 5});
        assertArrayEquals((double[]) vector.get("values"), new double[]{-48.20951464010758, 1.0, 1.0}, 1.e-7);

        // test missing string field - we expect in this case nothing to be in the vector although that might be a problem with the model...
        dataSource = createTestDataSource(null, null, 60);
        vector = vectorEntries.convert(dataSource).getAsMap();
        assertThat(((double[]) vector.get("values")).length, equalTo(3));
        assertThat(((int[]) vector.get("indices")).length, equalTo(3));
        assertArrayEquals((int[]) vector.get("indices"), new int[]{0, 4, 5});
        assertArrayEquals((double[]) vector.get("values"), new double[]{1.1724330344107299, 1.0, 1.0}, 1.e-7);
    }

    public void testGLMOnActualLookupMultipleStringValues() throws Exception {
        ModelFactories parser = ModelFactories.createDefaultModelFactories();
        DataSource dataSource = createTestDataSource(new String[]{"Self-emp-inc", "Private"}, null, 60);
        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/fake_lr_model_with_missing.xml");
        PMML pmml = ProcessPMMLHelper.parsePmml(pmmlString);
        ModelAndModelInputEvaluator<MapModelInput> fieldsToVectorAndModel = parser.buildFromPMML(pmml, 0);
        VectorRangesToVectorPMML vectorEntries = (VectorRangesToVectorPMML) fieldsToVectorAndModel.getVectorRangesToVector();
        @SuppressWarnings("unchecked")
        Map<String, Object> vector = vectorEntries.convert(dataSource).getAsMap();
        assertThat(((double[]) vector.get("values")).length, equalTo(4));
        assertThat(((int[]) vector.get("indices")).length, equalTo(4));
        assertArrayEquals((int[]) vector.get("indices"), new int[]{0, 1, 2, 5, });
        assertArrayEquals((double[]) vector.get("values"), new double[]{1.1724330344107299, 1.0, 1.0, 1.0}, 1.e-7);
    }


    @SuppressWarnings("unchecked")
    public void testTreeModelOnActualLookup() throws Exception {
        ModelFactories parser = ModelFactories.createDefaultModelFactories();
        DataSource dataSource = createTestDataSource(new String[]{"Self-emp-inc"}, "Prof-school", 60);
        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/tree-small-r.xml");
        PMML pmml = ProcessPMMLHelper.parsePmml(pmmlString);
        ModelAndModelInputEvaluator<MapModelInput> fieldsToVectorAndModel = parser.buildFromPMML(pmml, 0);
        VectorRangesToVectorPMML vectorEntries = (VectorRangesToVectorPMML) fieldsToVectorAndModel.getVectorRangesToVector();
        Map<String, Object> vector = vectorEntries.convert(dataSource).getAsMap();
        assertThat(vector.size(), equalTo(3));
        assertThat(((Number)((Set) vector.get("age_z")).iterator().next()).doubleValue(), closeTo(1.5702107070685085, 0.0));
        assertThat(((String)((Set) vector.get("education")).iterator().next()), equalTo("Prof-school"));
        assertThat(((String)((Set) vector.get("work")).iterator().next()), equalTo("Self-emp-inc"));

        // test missing values
        dataSource = createTestDataSource(null, null, null);
        vector = vectorEntries.convert(dataSource).getAsMap();
        assertThat(vector.size(), equalTo(3));
        assertThat(((Number)((Set) vector.get("age_z")).iterator().next()).doubleValue(), closeTo(-76.13993490863606, 0.0));
        assertThat(((String)((Set) vector.get("education")).iterator().next()), equalTo("too-lazy-to-study"));
        assertThat(((String)((Set) vector.get("work")).iterator().next()), equalTo("other"));
    }

}
