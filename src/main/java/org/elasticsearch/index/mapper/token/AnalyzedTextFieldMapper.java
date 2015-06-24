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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseMultiField;

/**
 * A {@link org.elasticsearch.index.mapper.FieldMapper} that takes a string and writes the tokens in that string
 * to a field.  In most ways the mapper acts just like an {@link org.elasticsearch.index.mapper.core.StringFieldMapper}.
 */
public class AnalyzedTextFieldMapper extends StringFieldMapper {
    public static final String CONTENT_TYPE = "analyzed_text";
    private final String nullValue;
    private final int ignoreAbove;
    private Boolean includeInAll;
    private NamedAnalyzer tokenAnalyzer;

    public static class Defaults extends StringFieldMapper.Defaults {
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        @SuppressWarnings("unchecked")
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            AnalyzedTextFieldMapper.Builder builder = new AnalyzedTextFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);
            builder.tokenized(false);
            builder.docValues(true);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(propNode.toString());
                } else if (propName.equals("search_quote_analyzer")) {
                    NamedAnalyzer analyzer = parserContext.analysisService().analyzer(propNode.toString());
                    if (analyzer == null) {
                        throw new MapperParsingException("Analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                    }
                    builder.searchQuotedAnalyzer(analyzer);
                } else if (propName.equals("position_offset_gap")) {
                    builder.positionOffsetGap(XContentMapValues.nodeIntegerValue(propNode, -1));
                    // we need to update to actual analyzers if they are not set in this case...
                    // so we can inject the position offset gap...
                    if (builder.indexAnalyzer() == null) {
                        builder.indexAnalyzer(parserContext.analysisService().defaultIndexAnalyzer());
                    }
                    if (builder.searchAnalyzer() == null) {
                        builder.searchAnalyzer(parserContext.analysisService().defaultSearchAnalyzer());
                    }
                    if (builder.searchQuotedAnalyzer() == null) {
                        builder.searchQuotedAnalyzer(parserContext.analysisService().defaultSearchQuoteAnalyzer());
                    }
                } else if (propName.equals("ignore_above")) {
                    builder.ignoreAbove(XContentMapValues.nodeIntegerValue(propNode, -1));
                } else if (propName.equals("token_analyzer")) {
                    builder.tokenAnalyzer(parserContext.analysisService().analyzer(propName));
                } else {
                    parseMultiField(builder, name, parserContext, propName, propNode);
                }
            }
            return builder;
        }
    }

    public static class Builder extends StringFieldMapper.Builder {

        String nullValue;

        NamedAnalyzer tokenAnalyzer = null;

        public Builder tokenAnalyzer(NamedAnalyzer tokenAnalyzer) {
            this.tokenAnalyzer = tokenAnalyzer;
            return this;
        }

        @Override
        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        public Builder(String name) {
            super(name);
        }

        @Override
        public AnalyzedTextFieldMapper build(BuilderContext context) {
            if (positionOffsetGap > 0) {
                indexAnalyzer = new NamedAnalyzer(indexAnalyzer, positionOffsetGap);
                searchAnalyzer = new NamedAnalyzer(searchAnalyzer, positionOffsetGap);
                searchQuotedAnalyzer = new NamedAnalyzer(searchQuotedAnalyzer, positionOffsetGap);
            }
            // if the field is not analyzed, then by default, we should omit norms and have docs only
            // index options, as probably what the user really wants
            // if they are set explicitly, we will use those values
            // we also change the values on the default field type so that toXContent emits what
            // differs from the defaults
            FieldType defaultFieldType = new FieldType(Defaults.FIELD_TYPE);
            if (fieldType.indexed() && !fieldType.tokenized()) {
                defaultFieldType.setOmitNorms(true);
                defaultFieldType.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY);
                if (!omitNormsSet && boost == Defaults.BOOST) {
                    fieldType.setOmitNorms(true);
                }
                if (!indexOptionsSet) {
                    fieldType.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY);
                }
            }
            defaultFieldType.freeze();
            AnalyzedTextFieldMapper fieldMapper = new AnalyzedTextFieldMapper(buildNames(context),
                    boost, fieldType, defaultFieldType, docValues, nullValue, indexAnalyzer, searchAnalyzer, searchQuotedAnalyzer,
                    positionOffsetGap, ignoreAbove, postingsProvider, docValuesProvider, similarity, normsLoading,
                    fieldDataSettings, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            fieldMapper.tokenAnalyzer(tokenAnalyzer);
            return fieldMapper;
        }

        public NamedAnalyzer indexAnalyzer() {
            return this.indexAnalyzer;
        }

        public NamedAnalyzer searchAnalyzer() {
            return this.searchAnalyzer;
        }

        public NamedAnalyzer searchQuotedAnalyzer() {
            return this.searchQuotedAnalyzer;
        }
    }

    private void tokenAnalyzer(NamedAnalyzer tokenAnalyzer) {
        this.tokenAnalyzer = tokenAnalyzer;
    }


    protected AnalyzedTextFieldMapper(Names names, float boost, FieldType fieldType, FieldType defaultFieldType, Boolean docValues,
                                      String nullValue, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer,
                                      NamedAnalyzer searchQuotedAnalyzer, int positionOffsetGap, int ignoreAbove,
                                      PostingsFormatProvider postingsFormat, DocValuesFormatProvider docValuesFormat,
                                      SimilarityProvider similarity, Loading normsLoading, @Nullable Settings fieldDataSettings,
                                      Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(names, boost, fieldType, defaultFieldType, docValues,
                nullValue, indexAnalyzer, searchAnalyzer,
                searchQuotedAnalyzer, positionOffsetGap, ignoreAbove,
                postingsFormat, docValuesFormat,
                similarity, normsLoading, fieldDataSettings,
                indexSettings, multiFields, copyTo);
        this.nullValue = nullValue;
        this.ignoreAbove = ignoreAbove;
    }

    public void includeInAll(java.lang.Boolean includeInAll) {
        this.includeInAll = includeInAll;
        super.includeInAll(includeInAll);
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        ValueAndBoost valueAndBoost = parseCreateFieldForString(context, nullValue, boost);
        if (valueAndBoost.value() == null) {
            return;
        }
        if (ignoreAbove > 0 && valueAndBoost.value().length() > ignoreAbove) {
            return;
        }
        if (context.includeInAll(includeInAll, this)) {
            context.allEntries().addText(names.fullName(), valueAndBoost.value(), valueAndBoost.boost());
        }

        Analyzer namedAnalyzer = (tokenAnalyzer == null) ? context.analysisService().defaultAnalyzer() : tokenAnalyzer;
        List<String> analyzedText = getAnalyzedText(namedAnalyzer.tokenStream(name(), valueAndBoost.value()));
        for (String s : analyzedText) {
            boolean added = false;
            if (fieldType.indexed() || fieldType.stored()) {
                Field field = new Field(names.indexName(), s, fieldType);
                field.setBoost(valueAndBoost.boost());
                fields.add(field);
                added = true;
            }
            if (hasDocValues()) {
                fields.add(new SortedSetDocValuesField(names.indexName(), new BytesRef(s)));
                added = true;
            }
            if (added == false) {
                context.ignoredValue(names.indexName(), s);
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
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        super.merge(mergeWith, mergeContext);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
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
