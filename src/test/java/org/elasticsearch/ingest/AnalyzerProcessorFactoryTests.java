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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;

public class AnalyzerProcessorFactoryTests extends ESTestCase {

    private final Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();

    private final AnalysisRegistry analysisRegistry = new AnalysisRegistry(new Environment(settings), emptyMap(), emptyMap(), emptyMap(),
            emptyMap());

    public void testCreate() throws Exception {
        try (IngestAnalysisService ingestAnalysisService = new IngestAnalysisService(settings)) {
            AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(ingestAnalysisService);
            ingestAnalysisService.setAnalysisRegistry(analysisRegistry);
            ingestAnalysisService.setAnalysisSettings(settings);

            Map<String, Object> config = new HashMap<>();
            config.put("field", "field1");
            config.put("target_field", "field2");
            config.put("analyzer", "standard");
            String processorTag = randomAsciiOfLength(10);

            AnalyzerProcessor analyzerProcessor = factory.create(null, processorTag, config);
            assertThat(analyzerProcessor.getTag(), equalTo(processorTag));
            assertThat(analyzerProcessor.getField(), equalTo("field1"));
            assertThat(analyzerProcessor.getTargetField(), equalTo("field2"));
            assertThat(analyzerProcessor.getAnalyzer(), equalTo("standard"));
        }
    }

    public void testCreateWithCustomAnalyzer() throws Exception {
        try (IngestAnalysisService ingestAnalysisService = new IngestAnalysisService(settings)) {
            AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(ingestAnalysisService);
            ingestAnalysisService.setAnalysisRegistry(analysisRegistry);
            final Settings ingestSettings = Settings.builder()
                    .put("analyzer.my_analyzer.type", "custom")
                    .put("analyzer.my_analyzer.tokenizer", "keyword")
                    .build();
            ingestAnalysisService.setAnalysisSettings(ingestSettings);

            Map<String, Object> config = new HashMap<>();
            config.put("field", "field1");
            config.put("analyzer", "my_analyzer");
            String processorTag = randomAsciiOfLength(10);

            AnalyzerProcessor analyzerProcessor = factory.create(null, processorTag, config);
            assertThat(analyzerProcessor.getTag(), equalTo(processorTag));
            assertThat(analyzerProcessor.getField(), equalTo("field1"));
            assertThat(analyzerProcessor.getTargetField(), equalTo("field1"));
            assertThat(analyzerProcessor.getAnalyzer(), equalTo("my_analyzer"));
        }
    }

    public void testCreateNoFieldPresent() throws Exception {
        try (IngestAnalysisService ingestAnalysisService = new IngestAnalysisService(settings)) {
            AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(ingestAnalysisService);
            ingestAnalysisService.setAnalysisRegistry(analysisRegistry);
            ingestAnalysisService.setAnalysisSettings(settings);

            Map<String, Object> config = new HashMap<>();
            config.put("analyzer", "standard");
            String processorTag = randomAsciiOfLength(10);
            try {
                factory.create(null, processorTag, config);
                fail("factory create should have failed");
            } catch (ElasticsearchParseException e) {
                assertThat(e.getMessage(), equalTo("[field] required property is missing"));
            }
        }
    }

    public void testCreateNoAnalyzerPresent() throws Exception {
        try (IngestAnalysisService ingestAnalysisService = new IngestAnalysisService(settings)) {
            AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(ingestAnalysisService);
            ingestAnalysisService.setAnalysisRegistry(analysisRegistry);
            ingestAnalysisService.setAnalysisSettings(settings);

            Map<String, Object> config = new HashMap<>();
            config.put("field", "field1");
            String processorTag = randomAsciiOfLength(10);
            try {
                factory.create(null, processorTag, config);
                fail("factory create should have failed");
            } catch (ElasticsearchParseException e) {
                assertThat(e.getMessage(), equalTo("[analyzer] required property is missing"));
            }
        }
    }

    public void testCreateWithUnknownAnalyzer() throws Exception {
        try (IngestAnalysisService ingestAnalysisService = new IngestAnalysisService(settings)) {
            AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(ingestAnalysisService);
            ingestAnalysisService.setAnalysisRegistry(analysisRegistry);
            ingestAnalysisService.setAnalysisSettings(settings);

            Map<String, Object> config = new HashMap<>();
            config.put("field", "field1");
            config.put("analyzer", "unknown_analyzer");
            String processorTag = randomAsciiOfLength(10);
            try {
                factory.create(null, processorTag, config);
                fail("factory create should have failed");
            } catch (IllegalArgumentException e) {
                assertThat(e.getMessage(), equalTo("Unknown analyzer [unknown_analyzer]"));
            }
        }
    }

    public void testUpdateCustomAnalyzer() throws Exception {
        try (IngestAnalysisService ingestAnalysisService = new IngestAnalysisService(settings)) {
            AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(ingestAnalysisService);
            ingestAnalysisService.setAnalysisRegistry(analysisRegistry);
            ingestAnalysisService.setAnalysisSettings(Settings.EMPTY);

            Map<String, Object> config = new HashMap<>();
            config.put("field", "field1");
            config.put("analyzer", "my_analyzer");

            String processorTag = randomAsciiOfLength(10);
            try {
                factory.create(null, processorTag, config);
                fail("factory create should have failed");
            } catch (IllegalArgumentException e) {
                assertThat(e.getMessage(), equalTo("Unknown analyzer [my_analyzer]"));
            }

            // Add new custom analyzer
            final Settings ingestSettings = Settings.builder()
                    .put("analyzer.my_analyzer.type", "custom")
                    .put("analyzer.my_analyzer.tokenizer", "keyword")
                    .build();
            ingestAnalysisService.setAnalysisSettings(ingestSettings);

            config = new HashMap<>();
            config.put("field", "field1");
            config.put("analyzer", "my_analyzer");
            AnalyzerProcessor analyzerProcessor = factory.create(null, processorTag, config);
            assertThat(analyzerProcessor.getTag(), equalTo(processorTag));
            assertThat(analyzerProcessor.getField(), equalTo("field1"));
            assertThat(analyzerProcessor.getTargetField(), equalTo("field1"));
            assertThat(analyzerProcessor.getAnalyzer(), equalTo("my_analyzer"));

            // Try adding bogus custom settings
            final Settings bogusIngestSettings = Settings.builder()
                    .put("analyzer.bogus_analyzer.type", "custom")
                    .put("analyzer.bogus_analyzer.tokenizer", "unknown_tokenizer")
                    .build();
            try {
                ingestAnalysisService.setAnalysisSettings(bogusIngestSettings);
                fail("Update should have failed");
            } catch (IllegalArgumentException e) {
                assertThat(e.getMessage(), startsWith("Couldn't parse ingest analyzer definition"));
            }

            // The old definitions should still work
            config = new HashMap<>();
            config.put("field", "field1");
            config.put("analyzer", "my_analyzer");
            analyzerProcessor = factory.create(null, processorTag, config);
            assertThat(analyzerProcessor.getTag(), equalTo(processorTag));
            assertThat(analyzerProcessor.getField(), equalTo("field1"));
            assertThat(analyzerProcessor.getTargetField(), equalTo("field1"));
            assertThat(analyzerProcessor.getAnalyzer(), equalTo("my_analyzer"));
        }
    }
}