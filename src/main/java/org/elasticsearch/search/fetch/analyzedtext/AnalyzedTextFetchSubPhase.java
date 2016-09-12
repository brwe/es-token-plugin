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

package org.elasticsearch.search.fetch.analyzedtext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class AnalyzedTextFetchSubPhase implements FetchSubPhase {

    public AnalyzedTextFetchSubPhase() {
    }

    public static final String NAME = "analyzed_text";

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        AnalyzedTextFetchBuilder request = (AnalyzedTextFetchBuilder)context.getSearchExt(NAME);
        if (request == null) {
            return;
        }
        Analyzer analyzer;
        boolean closeAnalyzer = false;
        String text = (String) context.lookup().source().extractValue(request.field());
        if (request.analyzer() != null) {
            analyzer = context.analysisService().analyzer(request.analyzer());
            if (analyzer == null) {
                throw new IllegalArgumentException("failed to find analyzer [" + request.analyzer() + "]");
            }
        } else if (request.tokenizer() != null) {
            TokenizerFactory tokenizerFactory;
            tokenizerFactory = context.analysisService().tokenizer(request.tokenizer());
            if (tokenizerFactory == null) {
                throw new IllegalArgumentException("failed to find tokenizer under [" + request.tokenizer() + "]");
            }
            TokenFilterFactory[] tokenFilterFactories = new TokenFilterFactory[0];
            if (request.tokenFilters() != null && request.tokenFilters().length > 0) {
                tokenFilterFactories = new TokenFilterFactory[request.tokenFilters().length];
                for (int i = 0; i < request.tokenFilters().length; i++) {
                    String tokenFilterName = request.tokenFilters()[i];
                    tokenFilterFactories[i] = context.analysisService().tokenFilter(tokenFilterName);
                    if (tokenFilterFactories[i] == null) {
                        throw new IllegalArgumentException("failed to find token filter under [" + tokenFilterName + "]");
                    }
                    if (tokenFilterFactories[i] == null) {
                        throw new IllegalArgumentException("failed to find token filter under [" + tokenFilterName + "]");
                    }
                }
            }

            CharFilterFactory[] charFilterFactories = new CharFilterFactory[0];
            if (request.charFilters() != null && request.charFilters().length > 0) {
                charFilterFactories = new CharFilterFactory[request.charFilters().length];
                for (int i = 0; i < request.charFilters().length; i++) {
                    String charFilterName = request.charFilters()[i];
                    charFilterFactories[i] = context.analysisService().charFilter(charFilterName);
                    if (charFilterFactories[i] == null) {
                        throw new IllegalArgumentException("failed to find token char under [" + charFilterName + "]");
                    }
                    if (charFilterFactories[i] == null) {
                        throw new IllegalArgumentException("failed to find token char under [" + charFilterName + "]");
                    }
                }
            }
            analyzer = new CustomAnalyzer(tokenizerFactory, charFilterFactories, tokenFilterFactories);
            closeAnalyzer = true;
        } else {
            analyzer = context.analysisService().defaultIndexAnalyzer();
        }
        if (analyzer == null) {
            throw new IllegalArgumentException("failed to find analyzer");
        }

        List<String> tokens = simpleAnalyze(analyzer, text, request.field());
        if (closeAnalyzer) {
            analyzer.close();
        }

        if (hitContext.hit().fieldsOrNull() == null) {
            hitContext.hit().fields(new HashMap<String, SearchHitField>());
        }
        SearchHitField hitField = hitContext.hit().fields().get(NAME);
        if (hitField == null) {
            hitField = new InternalSearchHitField(NAME, new ArrayList<>(1));
            hitContext.hit().fields().put(NAME, hitField);
        }

        hitField.values().add(tokens);
    }

    private static List<String> simpleAnalyze(Analyzer analyzer, String text, String field) {
        List<String> tokens = new ArrayList<>();
        try (TokenStream stream = analyzer.tokenStream(field, text)) {
            stream.reset();
            CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
            while (stream.incrementToken()) {
                tokens.add(term.toString());
            }
            stream.end();
        } catch (IOException e) {
            throw new ElasticsearchException("failed to analyze", e);
        }
        return tokens;
    }
}