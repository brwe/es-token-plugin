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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.hamcrest.Matchers;
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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.CoreMatchers.equalTo;

public class VectorizerPMMLSingleNodeTests extends ESSingleNodeTestCase {

    public void testOnActualLookup() throws Exception {
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
                .endObject()
                .endObject()
                .endObject();
        final IndexService indexService = createIndex("test", Settings.EMPTY, "test", mappings);
        final IndexFieldDataService ifdService = indexService.fieldData();
        IndexWriter writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));
        Document doc = new Document();
        doc.add(new StringField("work", "Self-emp-inc", Store.YES));
        FieldType fieldType = new FieldType();
        fieldType.setNumericType(FieldType.NumericType.INT);
        fieldType.setDocValuesType(DocValuesType.NUMERIC);
        doc.add(new IntField("age", 60, fieldType));
        writer.addDocument(doc);
        IndexReader reader = DirectoryReader.open(writer, true);
        LeafReaderContext leafReaderContext = reader.leaves().get(0);
        LeafDocLookup docLookup = new SearchLookup(indexService.mapperService(), ifdService, new String[]{"test"}).getLeafSearchLookup(leafReaderContext).doc();
        final String pmmlString = copyToStringFromClasspath("/org/elasticsearch/script/fake_lr_model_with_missing.xml");
        PMML pmml = AccessController.doPrivileged(new PrivilegedAction<PMML>() {
            public PMML run() {
                try (InputStream is = new ByteArrayInputStream(pmmlString.getBytes(Charset.defaultCharset()))) {
                    Source transformedSource = ImportFilter.apply(new InputSource(is));
                    return JAXBUtil.unmarshalPMML(transformedSource);
                } catch (SAXException e) {
                    throw new ElasticsearchException("could not convert xml to pmml model", e);
                } catch (JAXBException e) {
                    throw new ElasticsearchException("could not convert xml to pmml model", e);
                } catch (IOException e) {
                    throw new ElasticsearchException("could not convert xml to pmml model", e);
                }
            }
        });
        docLookup.setDocument(0);
        VectorEntriesPMML vectorEntries = new VectorEntriesPMML(pmml, 0);
        Map<String, Object> vector = (Map<String, Object>) vectorEntries.vector(docLookup, null, null, null);
        assertThat(((double[]) vector.get("values")).length, equalTo(2));
        assertThat(((int[]) vector.get("indices")).length, equalTo(2));
        assertArrayEquals((int[]) vector.get("indices"), new int[]{0, 2});
        assertThat(1.1724330344107299, Matchers.closeTo(((double[]) vector.get("values"))[0], 1.e-7));
        reader.close();

        // test missing values
        writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));
        doc = new Document();
        doc.add(new StringField("work", "Self-emp-inc", Store.YES));

        writer.addDocument(doc);
        reader = DirectoryReader.open(writer, true);
        leafReaderContext = reader.leaves().get(0);
        docLookup = new SearchLookup(indexService.mapperService(), ifdService, new String[]{"test"}).getLeafSearchLookup(leafReaderContext).doc();
        docLookup.setDocument(0);
        vectorEntries = new VectorEntriesPMML(pmml, 0);
        vector = (Map<String, Object>) vectorEntries.vector(docLookup, null, null, null);
        assertThat(((double[]) vector.get("values")).length, equalTo(2));
        assertThat(((int[]) vector.get("indices")).length, equalTo(2));
        assertArrayEquals((int[]) vector.get("indices"), new int[]{0, 2});
        assertThat(-48.20951464010758, Matchers.closeTo(((double[]) vector.get("values"))[0], 1.e-7));
        reader.close();

        // test missing string field - we expect in this case nothig to be in the vector although that might be a problem with the model...
        writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));
        doc = new Document();
        fieldType = new FieldType();
        fieldType.setNumericType(FieldType.NumericType.INT);
        fieldType.setDocValuesType(DocValuesType.NUMERIC);
        doc.add(new IntField("age", 60, fieldType));
        writer.addDocument(doc);
        reader = DirectoryReader.open(writer, true);
        leafReaderContext = reader.leaves().get(0);
        docLookup = new SearchLookup(indexService.mapperService(), ifdService, new String[]{"test"}).getLeafSearchLookup(leafReaderContext).doc();
        docLookup.setDocument(0);
        vectorEntries = new VectorEntriesPMML(pmml, 0);
        vector = (Map<String, Object>) vectorEntries.vector(docLookup, null, null, null);
        assertThat(((double[]) vector.get("values")).length, equalTo(2));
        assertThat(((int[]) vector.get("indices")).length, equalTo(2));
        assertArrayEquals((int[]) vector.get("indices"), new int[]{0, 4});
        assertThat(1.1724330344107299, Matchers.closeTo(((double[]) vector.get("values"))[0], 1.e-7));
        reader.close();
    }
}
