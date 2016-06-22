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

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.RAMDirectory;
import org.dmg.pmml.PMML;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.script.modelinput.VectorRangesToVectorPMML;
import org.elasticsearch.script.pmml.PMMLModelScriptEngineService;
import org.elasticsearch.script.pmml.ProcessPMMLHelper;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;

public class VectorizerPMMLSingleNodeTests extends ESSingleNodeTestCase {
    IndexFieldDataService ifdService;
    IndexService indexService;

    private LeafDocLookup indexDoc(String[] work, String education, Integer age) throws IOException {
        IndexWriter writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));
        Document doc = new Document();
        if (work != null) {
            for (String value : work) {
                doc.add(new StringField("work", value, Store.YES));
            }
        }
        if (education != null) {
            doc.add(new StringField("education", education, Store.YES));
        }
        if (age!=null) {
            FieldType fieldType = new FieldType();
            fieldType.setNumericType(FieldType.NumericType.INT);
            fieldType.setDocValuesType(DocValuesType.NUMERIC);
            doc.add(new IntField("age", age, fieldType));
        }
        writer.addDocument(doc);
        IndexReader reader = DirectoryReader.open(writer, true);
        LeafReaderContext leafReaderContext = reader.leaves().get(0);
        LeafDocLookup docLookup = new SearchLookup(indexService.mapperService(), ifdService, new String[]{"test"}).getLeafSearchLookup(leafReaderContext).doc();
        reader.close();
        docLookup.setDocument(0);
        return docLookup;
    }
    public void testGLMOnActualLookup() throws Exception {
        setupServices();

        LeafDocLookup docLookup = indexDoc(new String[]{"Self-emp-inc"}, null, 60);
        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/fake_lr_model_with_missing.xml");
        PMML pmml = ProcessPMMLHelper.parsePmml(pmmlString);
        PMMLModelScriptEngineService.FieldsToVectorAndModel fieldsToVectorAndModel = PMMLModelScriptEngineService.getFeaturesAndModelFromFullPMMLSpec(pmml, 0);
        VectorRangesToVectorPMML vectorEntries = (VectorRangesToVectorPMML
                ) fieldsToVectorAndModel.getVectorRangesToVector();
        Map<String, Object> vector = (Map<String, Object>) vectorEntries.vector(docLookup, null, null, null);
        assertThat(((double[]) vector.get("values")).length, equalTo(3));
        assertThat(((int[]) vector.get("indices")).length, equalTo(3));
        assertArrayEquals((int[]) vector.get("indices"), new int[]{0, 2, 5});
        assertArrayEquals((double[]) vector.get("values"), new double[]{1.1724330344107299, 1.0, 1.0}, 1.e-7);

        // test missing values
        docLookup = indexDoc(new String[]{"Self-emp-inc"}, null, null);
        docLookup.setDocument(0);
        vector = (Map<String, Object>) vectorEntries.vector(docLookup, null, null, null);
        assertThat(((double[]) vector.get("values")).length, equalTo(3));
        assertThat(((int[]) vector.get("indices")).length, equalTo(3));
        assertArrayEquals((int[]) vector.get("indices"), new int[]{0, 2, 5});
        assertArrayEquals((double[]) vector.get("values"), new double[]{-48.20951464010758, 1.0, 1.0}, 1.e-7);

        // test missing string field - we expect in this case nothing to be in the vector although that might be a problem with the model...
        docLookup = indexDoc(null, null, 60);
        vector = (Map<String, Object>) vectorEntries.vector(docLookup, null, null, null);
        assertThat(((double[]) vector.get("values")).length, equalTo(3));
        assertThat(((int[]) vector.get("indices")).length, equalTo(3));
        assertArrayEquals((int[]) vector.get("indices"), new int[]{0, 4, 5});
        assertArrayEquals((double[]) vector.get("values"), new double[]{1.1724330344107299, 1.0, 1.0}, 1.e-7);
    }

    public void testGLMOnActualLookupMultipleStringValues() throws Exception {
        setupServices();

        LeafDocLookup docLookup = indexDoc(new String[]{"Self-emp-inc", "Private"}, null, 60);
        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/fake_lr_model_with_missing.xml");
        PMML pmml = ProcessPMMLHelper.parsePmml(pmmlString);
        PMMLModelScriptEngineService.FieldsToVectorAndModel fieldsToVectorAndModel = PMMLModelScriptEngineService.getFeaturesAndModelFromFullPMMLSpec(pmml, 0);
        VectorRangesToVectorPMML vectorEntries = (VectorRangesToVectorPMML
                ) fieldsToVectorAndModel.getVectorRangesToVector();
        Map<String, Object> vector = (Map<String, Object>) vectorEntries.vector(docLookup, null, null, null);
        assertThat(((double[]) vector.get("values")).length, equalTo(4));
        assertThat(((int[]) vector.get("indices")).length, equalTo(4));
        assertArrayEquals((int[]) vector.get("indices"), new int[]{0, 1, 2, 5, });
        assertArrayEquals((double[]) vector.get("values"), new double[]{1.1724330344107299, 1.0, 1.0, 1.0}, 1.e-7);
    }


    public void testTreeModelOnActualLookup() throws Exception {
        setupServices();

        LeafDocLookup docLookup = indexDoc(new String[]{"Self-emp-inc"}, "Prof-school", 60);
        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/tree-small-r.xml");
        PMML pmml = ProcessPMMLHelper.parsePmml(pmmlString);
        PMMLModelScriptEngineService.FieldsToVectorAndModel fieldsToVectorAndModel = PMMLModelScriptEngineService.getFeaturesAndModelFromFullPMMLSpec(pmml, 0);
        VectorRangesToVectorPMML vectorEntries = (VectorRangesToVectorPMML
                ) fieldsToVectorAndModel.getVectorRangesToVector();
        Map<String, Object> vector = (Map<String, Object>) vectorEntries.vector(docLookup, null, null, null);
        assertThat(vector.size(), equalTo(3));
        assertThat(((Number)((Set) vector.get("age_z")).iterator().next()).doubleValue(), closeTo(1.5702107070685085, 0.0));
        assertThat(((String)((Set) vector.get("education")).iterator().next()), equalTo("Prof-school"));
        assertThat(((String)((Set) vector.get("work")).iterator().next()), equalTo("Self-emp-inc"));

        // test missing values
        docLookup = indexDoc(null, null, null);
        docLookup.setDocument(0);
        vector = (Map<String, Object>) vectorEntries.vector(docLookup, null, null, null);
        assertThat(vector.size(), equalTo(3));
        assertThat(((Number)((Set) vector.get("age_z")).iterator().next()).doubleValue(), closeTo(-76.13993490863606, 0.0));
        assertThat(((String)((Set) vector.get("education")).iterator().next()), equalTo("too-lazy-to-study"));
        assertThat(((String)((Set) vector.get("work")).iterator().next()), equalTo("other"));
    }

    protected void setupServices() throws IOException {
        XContentBuilder mappings = jsonBuilder();
        mappings.startObject()
                .startObject("test")
                .startObject("properties")
                .startObject("age")
                .field("type", "integer")
                .field("doc_values", "true")
                .endObject()
                .startObject("work")
                .field("type", "string")
                .field("analyzer", "keyword")
                .endObject()
                .startObject("education")
                .field("type", "string")
                .field("analyzer", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        indexService = createIndex("test", Settings.EMPTY, "test", mappings);
        ifdService = indexService.fieldData();
    }


}
