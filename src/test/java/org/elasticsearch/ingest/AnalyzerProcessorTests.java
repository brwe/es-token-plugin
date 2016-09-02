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

package org.elasticsearch.ingest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class AnalyzerProcessorTests extends ESTestCase {


    private IngestAnalysisService ingestAnalysisService = new IngestAnalysisService(Settings.EMPTY);

    private AnalyzerProcessor.Factory analyzerProcessorFactory;

    @Before
    public void before() {
        Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        AnalysisRegistry analysisRegistry = new AnalysisRegistry(new Environment(settings), emptyMap(), emptyMap(),
                emptyMap(), emptyMap());
        ingestAnalysisService.setAnalysisRegistry(analysisRegistry);
        ingestAnalysisService.setAnalysisSettings(settings);
        analyzerProcessorFactory = new AnalyzerProcessor.Factory(ingestAnalysisService);
    }

    @After
    public void after() {
        ingestAnalysisService.close();
    }

    private AnalyzerProcessor newAnalyzerProcessor(String field, String targetField,
                                                   String analyzer) throws Exception {
        Map<String, Object> params = new HashMap<>();
        if (field != null) {
            params.put("field", field);
        }
        if (targetField != null) {
            params.put("target_field", targetField);
        }
        if (analyzer != null) {
            params.put("analyzer", analyzer);
        }
        return analyzerProcessorFactory.create(null, randomAsciiOfLength(10), params);
    }

    public void testAnalysis() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "This is a test.");
        Processor processor = newAnalyzerProcessor(fieldName, null, "standard");
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(Arrays.asList("this", "is", "a", "test")));
    }

    public void testAnalysisMultiValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        List<Object> multiValueField = new ArrayList<>();
        multiValueField.add("This is");
        multiValueField.add("a test.");
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, multiValueField);
        Processor processor = newAnalyzerProcessor(fieldName, null, "standard");
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(Arrays.asList("this", "is", "a", "test")));
    }

    public void testAnalysisWithTargetField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "This is a test.");
        String targetFieldName = randomAsciiOfLength(10);
        Processor processor = newAnalyzerProcessor(fieldName, targetFieldName, "standard");
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(targetFieldName, List.class), equalTo(Arrays.asList("this", "is", "a", "test")));
    }

    public void testAnalysisFieldNotFound() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = newAnalyzerProcessor(fieldName, null, "standard");
        try {
            processor.execute(ingestDocument);
            fail("analyzer processor should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
        }
    }

    public void testAnalysisNullValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        Processor processor = newAnalyzerProcessor("field", null, "standard");
        try {
            processor.execute(ingestDocument);
            fail("analyzer processor should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [field] is null, cannot be analyzed."));
        }
    }

    public void testAnalysisNonStringValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        ingestDocument.setFieldValue(fieldName, randomInt());
        Processor processor = newAnalyzerProcessor(fieldName, null, "standard");
        try {
            processor.execute(ingestDocument);
            fail("analyzer processor should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [" + fieldName + "] has type [java.lang.Integer] and cannot be analyzed"));
        }
    }

    public void testAnalysisAppendable() throws Exception {
        Processor analyzerProcessor = newAnalyzerProcessor("text", null, "standard");
        Map<String, Object> source = new HashMap<>();
        source.put("text", "This is a test.");
        IngestDocument ingestDocument = new IngestDocument(source, new HashMap<>());
        analyzerProcessor.execute(ingestDocument);
        @SuppressWarnings("unchecked")
        List<String> flags = (List<String>) ingestDocument.getFieldValue("text", List.class);
        assertThat(flags, equalTo(Arrays.asList("this", "is", "a", "test")));
        ingestDocument.appendFieldValue("text", "and this");
        assertThat(ingestDocument.getFieldValue("text", List.class), equalTo(Arrays.asList("this", "is", "a", "test",
                "and this")));
    }
}