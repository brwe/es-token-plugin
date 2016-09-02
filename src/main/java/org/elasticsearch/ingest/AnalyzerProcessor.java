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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ingest.IngestAnalysisService.AnalysisServiceHolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Processor that splits fields content into tokens using specified analyzer.
 * New field value will be an array containing all generated tokens.
 * Throws exception if the field is null or a type other than string.
 */
public class AnalyzerProcessor extends AbstractProcessor {

    public static final String TYPE = "analyzer";

    private final String targetField;

    private final String field;

    private final String analyzer;

    private final IngestAnalysisService ingestAnalysisService;

    AnalyzerProcessor(String tag, String field, String targetField, String analyzer, IngestAnalysisService ingestAnalysisService) {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.analyzer = analyzer;
        this.ingestAnalysisService = ingestAnalysisService;
    }

    String getField() {
        return field;
    }

    String getTargetField() {
        return targetField;
    }

    String getAnalyzer() {
        return analyzer;
    }

    @Override
    public void execute(IngestDocument document) {
        Object oldVal = document.getFieldValue(field, Object.class);
        if (oldVal == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot be analyzed.");
        }
        List<String> tokenList = new ArrayList<>();
        if (oldVal instanceof String) {
            analyze(tokenList, (String) oldVal);
        } else if (oldVal instanceof ArrayList) {
            for (Object obj : (ArrayList) oldVal) {
                analyze(tokenList, obj.toString());
            }
        } else {
            throw new IllegalArgumentException("field [" + field + "] has type [" + oldVal.getClass().getName() +
                    "] and cannot be analyzed");
        }
        document.setFieldValue(targetField, tokenList);
    }

    private void analyze(List<String> tokenList, String val) {
        AnalysisServiceHolder analysisServiceHolder = ingestAnalysisService.acquireAnalysisServiceHolder();
        try {
            try (TokenStream stream = analysisServiceHolder.tokenStream(analyzer, field, val)) {
                stream.reset();
                CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
                while (stream.incrementToken()) {
                    tokenList.add(term.toString());
                }
                stream.end();
            } catch (IOException e) {
                throw new ElasticsearchException("failed to analyze field [" + field + "]", e);
            }
        } finally {
            analysisServiceHolder.release();
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory {

        private final IngestAnalysisService ingestAnalysisService;

        public Factory(IngestAnalysisService ingestAnalysisService) {
            this.ingestAnalysisService = ingestAnalysisService;
        }

        @Override
        public AnalyzerProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                        Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", field);
            String analyzer = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "analyzer");
            AnalysisServiceHolder analysisServiceHolder = ingestAnalysisService.acquireAnalysisServiceHolder();
            try {
                if (analysisServiceHolder.hasAnalyzer(analyzer) == false) {
                    throw new IllegalArgumentException("Unknown analyzer [" + analyzer + "]");
                }
                return new AnalyzerProcessor(processorTag, field, targetField, analyzer, ingestAnalysisService);
            } finally {
                analysisServiceHolder.release();
            }
        }
    }

}

