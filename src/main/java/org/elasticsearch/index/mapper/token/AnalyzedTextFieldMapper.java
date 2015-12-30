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

package org.elasticsearch.index.mapper.token;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.StringFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A {@link org.elasticsearch.index.mapper.FieldMapper} that takes a string and writes the tokens in that string
 * to a field.  In most ways the mapper acts just like an {@link org.elasticsearch.index.mapper.core.StringFieldMapper}.
 */
public class AnalyzedTextFieldMapper extends StringFieldMapper {
    public static final String CONTENT_TYPE = "analyzed_text";

    public static class TypeParser extends StringFieldMapper.TypeParser {
        @Override
        @SuppressWarnings("unchecked")
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            StringFieldMapper.Builder stringBuilder = (StringFieldMapper.Builder) super.parse(name, node, parserContext);
            return new Builder(stringBuilder);
        }
    }

    public static class Builder extends Mapper.Builder {

        public Builder(StringFieldMapper.Builder builder) {
            super(builder.name());
            this.builder = builder;
        }

        @Override
        public AnalyzedTextFieldMapper build(BuilderContext context) {
            StringFieldMapper mapper = (StringFieldMapper) builder.build(context);
            MappedFieldType fieldType = mapper.fieldType().clone();
            fieldType.setTokenized(false);
            fieldType.setDocValuesType(DocValuesType.SORTED);
            AnalyzedTextFieldMapper fieldMapper = new AnalyzedTextFieldMapper(mapper.simpleName(), fieldType, fieldType,
                    mapper.getPositionIncrementGap(), mapper.getIgnoreAbove(), context.indexSettings(), null, null);
            return fieldMapper;
        }


    }

    NamedAnalyzer tokenAnalyzer;

    protected AnalyzedTextFieldMapper(String name, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                      int positionIncrementGap, int ignoreAbove,
                                      Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(name, fieldType, defaultFieldType,
                positionIncrementGap, ignoreAbove,
                indexSettings, multiFields, copyTo);
        this.tokenAnalyzer = fieldType.indexAnalyzer();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        ValueAndBoost valueAndBoost = parseCreateFieldForString(context, null, 1.0f);
        if (valueAndBoost.value() == null) {
            return;
        }

        Analyzer namedAnalyzer = (tokenAnalyzer == null) ? context.analysisService().defaultIndexAnalyzer() : tokenAnalyzer;
        List<String> analyzedText = getAnalyzedText(namedAnalyzer.tokenStream(name(), valueAndBoost.value()));
        for (String s : analyzedText) {
            boolean added = false;

            if (hasDocValues()) {
                fields.add(new SortedSetDocValuesField(name(), new BytesRef(s)));
                added = true;
            }
            if (added == false) {
                context.ignoredValue(name(), s);
            }
        }
    }

    public boolean hasDocValues() {
        return true;
    }

    static List<String> getAnalyzedText(TokenStream tokenStream) throws IOException {
        try {
            List<String> analyzedText = new ArrayList<>();
            CharTermAttribute terms = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();

            while (tokenStream.incrementToken()) {
                analyzedText.add(new String(terms.toString()));
            }
            tokenStream.end();
            return analyzedText;
        } finally {
            tokenStream.close();
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }


    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
    }

    @Override
    public boolean isGenerated() {
        return true;
    }

}
